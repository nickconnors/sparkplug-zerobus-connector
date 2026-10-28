"""ConfirmedWriter and the bridge's confirmed-writes path."""
from __future__ import annotations

import asyncio
import inspect
import json

import pytest

from conftest import FIXTURES
from sparkplug_connector.core import bridge as core_bridge
from sparkplug_connector.core.backpressure import BackpressureManager
from sparkplug_connector.core.confirmed_writer import (MAX_BACKOFF_S, MAX_RECORD_BYTES, ConfirmedWriter, ZerobusRowStreams,
                                                     take_from_backpressure)
from sparkplug_connector.sparkplug.raw_row import COLUMNS, convert


class ManualClock:
    """A clock the test moves by hand; the writer's backoff sleeps advance it."""

    def __init__(self):
        self.now = 1_790_694_000.0

    def __call__(self) -> float:
        return self.now


class FakeStreams:
    """Streams that record what they were given and fail on demand."""

    def __init__(self):
        self.ingested: list[list[str]] = []
        self.opens = self.closes = self.fail_opens = self.fail_waits = 0
        self.during_wait = lambda: None

    async def open(self):
        self.opens += 1
        if self.fail_opens:
            self.fail_opens -= 1
            raise ConnectionError("workspace unreachable")
        return FakeStream(self)


class FakeStream:
    def __init__(self, owner: FakeStreams):
        self.owner = owner

    async def ingest(self, records):
        self.owner.ingested.append(list(records))
        return len(self.owner.ingested) - 1

    async def wait_for(self, offset):
        self.owner.during_wait()
        if self.owner.fail_waits:
            self.owner.fail_waits -= 1
            raise TimeoutError("ack timeout")

    async def close(self):
        self.owner.closes += 1


def row(msg_id: str, msg: str = '{"seq":1}') -> dict:
    return {"msg_id": msg_id, "msg_topic": "spBv1.0/G/NDATA/E", "namespace": "spBv1.0", "group_id": "G", "message_type": "NDATA",
            "edge_node_id": "E", "device_id": None, "msg": msg, "inserted_at": "1790694245123"}


@pytest.fixture
def setup():
    queue: list[dict] = []
    streams, clock, sleeps = FakeStreams(), ManualClock(), []

    async def take(n, linger, wait):
        batch = queue[:n]
        del queue[:n]
        return batch

    async def sleep(d):
        sleeps.append(d)
        clock.now += d

    writer = ConfirmedWriter(take, streams.open, batch_size=500, clock=clock, sleep=sleep,
                             queue_depth=lambda: len(queue), queue_high_water=3)
    return writer, queue, streams, clock, sleeps


async def test_writes_a_batch_and_reports_healthy(setup):
    writer, queue, streams, clock, _ = setup
    queue += [row("1"), row("2")]
    await writer.step()
    assert len(streams.ingested) == 1 and len(streams.ingested[0]) == 2
    assert all('"msg_id"' in j for j in streams.ingested[0])
    s = writer.status()
    assert (s.written, s.failing_since, s.pending_since, s.last_success, s.stream_open) == (2, None, None, clock.now, True)


async def test_keeps_the_batch_until_acknowledged_and_resends_it_on_a_fresh_stream(setup):
    writer, queue, streams, _, _ = setup
    queue.append(row("a"))
    streams.fail_waits = 1
    await writer.step()                                   # ingest, ack fails
    s = writer.status()
    assert (s.written, s.batch_in_flight, s.stream_open, streams.closes) == (0, 1, False, 1)
    await writer.step()                                   # new stream, same batch
    assert streams.opens == 2 and len(streams.ingested) == 2
    assert streams.ingested[1] == streams.ingested[0]     # same rows, same msg_ids
    assert writer.status().written == 1 and writer.status().failing_since is None


async def test_failing_since_marks_the_first_failure_and_backs_off_exponentially(setup):
    writer, _, streams, clock, sleeps = setup
    streams.fail_opens = 4
    start = clock.now
    for _ in range(4):
        await writer.step()
    s = writer.status()
    assert s.failing_since == start and s.failures == 4 and "workspace unreachable" in s.last_error
    assert sleeps == [1, 2, 4, 8]
    await writer.step()                                   # opens now
    assert writer.status().failing_since is None


async def test_the_last_error_and_its_time_stay_after_recovery(setup):
    """For the pages: an error is current only while failing_since is set; last_error (and when) stays as history."""
    writer, _, streams, clock, _ = setup
    streams.fail_opens = 2
    start = clock.now
    await writer.step()                                   # fails, then backs off 1 s
    await writer.step()                                   # fails again (then backs off 2 s)
    last = start + 1                                      # the second failure's time
    s = writer.status()
    assert s.failing_since is not None and s.last_error_at == last
    await writer.step()                                   # opens, writes
    s = writer.status()
    assert s.failing_since is None and "workspace unreachable" in s.last_error and s.last_error_at == last


async def test_backoff_is_capped(setup):
    writer, _, streams, _, sleeps = setup
    streams.fail_opens = 10
    for _ in range(10):
        await writer.step()
    assert sleeps[-1] == MAX_BACKOFF_S


async def test_pending_since_is_visible_while_waiting_for_the_ack(setup):
    writer, queue, streams, clock, _ = setup
    queue.append(row("x"))
    before, seen = clock.now, []

    def during_wait():
        clock.now += 40
        seen.append(writer.status())
    streams.during_wait = during_wait
    await writer.step()
    assert seen[0].pending_since == before                # the host state sees a 40 s old pending batch
    assert writer.status().pending_since is None


async def test_pending_since_spans_retries_of_the_same_batch(setup):
    writer, queue, streams, clock, _ = setup
    queue.append(row("x"))
    streams.fail_waits = 2
    first = clock.now
    await writer.step()
    await writer.step()
    assert writer.status().pending_since == first
    await writer.step()
    assert writer.status().pending_since is None and writer.status().written == 1


# ----------------------------------------------------------------------------- rows Zerobus won't take, queue high water

async def test_a_row_over_the_zerobus_limit_is_skipped_and_counted_not_retried_forever(setup):
    writer, queue, streams, _, _ = setup
    queue += [row("small"), row("big", msg=json.dumps({"pad": "x" * MAX_RECORD_BYTES})), row("small 2")]
    await writer.step()
    assert [json.loads(j)["msg_id"] for j in streams.ingested[0]] == ["small", "small 2"]
    assert writer.status().rows_too_large == 1 and writer.status().written == 2


async def test_while_opens_work_but_writes_keep_failing_it_stays_failing_and_backs_off(setup):
    writer, queue, streams, clock, sleeps = setup
    queue.append(row("x"))
    streams.fail_waits = 3                                # every resend's acknowledgement fails
    first = clock.now
    await writer.step()                                   # opens (healthy: nothing to resend yet), write fails
    probe = writer.status().last_success
    for _ in range(2):
        await writer.step()                               # a fresh stream for the resend isn't a success
    assert writer.status().failing_since == first and writer.status().last_success == probe
    assert sleeps == [1, 2, 4]
    await writer.step()
    s = writer.status()
    assert (s.written, s.failing_since, s.last_success) == (1, None, clock.now)


async def test_an_open_with_nothing_to_resend_is_healthy(setup):
    writer, _, _, clock, _ = setup
    await writer.step()                                   # open, then an empty take
    assert writer.status().last_success == clock.now and writer.status().failing_since is None


async def test_a_row_that_cant_be_a_json_record_is_skipped_and_counted(setup):
    writer, queue, streams, _, _ = setup
    queue += [row("1"), {**row("bad"), "msg": object()}, row("2")]
    await writer.step()
    assert [json.loads(j)["msg_id"] for j in streams.ingested[0]] == ["1", "2"]
    assert writer.status().rows_unconvertible == 1 and writer.status().written == 2


async def test_a_row_with_nan_or_infinity_is_skipped_not_written_as_invalid_json(setup):
    """json.dumps writes them as bare NaN / Infinity tokens, which aren't JSON: Zerobus would refuse the batch,
    resent for good."""
    writer, queue, streams, _, _ = setup
    queue += [row("1"), {**row("nan"), "value_num": float("nan")}, {**row("inf"), "value_num": float("inf")}, row("2")]
    await writer.step()
    assert [json.loads(j)["msg_id"] for j in streams.ingested[0]] == ["1", "2"]
    assert writer.status().rows_unconvertible == 2

async def test_a_failing_take_is_retried_and_doesnt_end_the_writer():
    queue, streams, calls = [row("1")], FakeStreams(), []

    async def take(n, linger, wait):
        calls.append(n)
        if len(calls) == 1:
            raise OSError("spool file unreadable")
        batch = queue[:n]
        del queue[:n]
        return batch

    async def no_sleep(_):
        pass

    writer = ConfirmedWriter(take, streams.open, sleep=no_sleep, queue_depth=lambda: len(queue))
    writer.finish(5)
    await asyncio.wait_for(writer.run(), 2)
    assert writer.status().written == 1 and writer.status().failures == 1
    assert "spool file unreadable" in writer.status().last_error


async def test_after_a_failed_take_the_next_good_one_clears_the_failure():
    # no data may arrive (an OFFLINE primary host gets none), so no acknowledgement would clear it
    streams, calls = FakeStreams(), []

    async def take(n, linger, wait):
        calls.append(n)
        if len(calls) == 1:
            raise OSError("spool file unreadable")
        return []

    async def no_sleep(_):
        pass

    writer = ConfirmedWriter(take, streams.open, sleep=no_sleep)
    await writer.step()                                   # opens, the take fails
    assert writer.status().failing_since is not None
    await writer.step()                                   # the take works: nothing queued
    assert writer.status().failing_since is None and writer.status().last_success is not None


async def test_queue_above_its_high_water_mark_shows_in_the_status(setup):
    writer, queue, _, _, _ = setup
    queue += [row(str(i)) for i in range(2)]
    assert writer.status().queue_high_water is False
    queue.append(row("3"))
    assert writer.status().queue_high_water is True and writer.status().queue_depth == 3


# ----------------------------------------------------------------------------- run, drain, stop

async def test_run_drains_the_queue_then_stops():
    queue = [row(str(i)) for i in range(5)]
    streams = FakeStreams()

    async def take(n, linger, wait):
        batch = queue[:2]
        del queue[:2]
        if not batch:
            await asyncio.sleep(0.01)
        return batch

    writer = ConfirmedWriter(take, streams.open, queue_depth=lambda: len(queue))
    task = asyncio.create_task(writer.run())
    writer.finish(5)
    await asyncio.wait_for(task, 2)
    assert writer.status().written == 5 and streams.closes == 1


async def test_the_drain_carries_on_after_a_take_of_only_rows_it_skips():
    queue = [row("big", msg=json.dumps({"pad": "x" * MAX_RECORD_BYTES})), row("after")]
    streams = FakeStreams()

    async def take(n, linger, wait):
        batch = queue[:n]
        del queue[:n]
        return batch

    writer = ConfirmedWriter(take, streams.open, batch_size=1, queue_depth=lambda: len(queue))
    writer.finish(5)
    await asyncio.wait_for(writer.run(), 2)
    assert [json.loads(j)["msg_id"] for b in streams.ingested for j in b] == ["after"]
    assert writer.status().rows_too_large == 1


async def test_finish_gives_up_after_its_timeout_while_writes_fail(setup):
    writer, queue, streams, clock, _ = setup
    queue.append(row("x"))
    streams.fail_opens = 1000
    writer.finish(10)
    await asyncio.wait_for(writer.run(), 2)                # backoff sleeps advance the manual clock past 10 s
    assert writer.status().written == 0 and writer.status().failures >= 4


async def test_take_from_backpressure_fills_a_batch_and_waits_for_the_first_record():
    bp = BackpressureManager({})
    assert await take_from_backpressure(bp, dict, 10, 0.05, 0.05) == []
    for i in range(3):
        await bp.enqueue({"n": i})
    assert [r["n"] for r in await take_from_backpressure(bp, dict, 2, 0.05, 1)] == [0, 1]
    assert [r["n"] for r in await take_from_backpressure(bp, dict, 2, 0.05, 1)] == [2]


async def test_take_from_backpressure_keeps_the_rows_taken_before_the_queue_fails():
    class Flaky:
        def __init__(self):
            self.n = 0

        async def dequeue(self):
            self.n += 1
            if self.n <= 2:
                return {"n": self.n}
            raise OSError("spool file unreadable")

    assert [r["n"] for r in await take_from_backpressure(Flaky(), dict, 10, 0.05, 1)] == [1, 2]

    class Broken:
        async def dequeue(self):
            raise OSError("spool file unreadable")

    with pytest.raises(OSError):                            # nothing taken: the take fails (and is retried)
        await take_from_backpressure(Broken(), dict, 10, 0.05, 1)


async def test_take_from_backpressure_leaves_out_a_record_it_cant_convert():
    bp = BackpressureManager({})
    for i in range(3):
        await bp.enqueue({"n": i, "msg_topic": f"t{i}"})
    bad = []

    def to_record(record):
        if record["n"] == 1:
            raise KeyError("column")
        return record
    got = await take_from_backpressure(bp, to_record, 10, 0.05, 1, lambda topic, e: bad.append(topic))
    assert [r["n"] for r in got] == [0, 2] and bad == ["t1"]


# ----------------------------------------------------------------------------- the Zerobus SDK adapter

async def test_zerobus_row_streams_open_a_json_stream_to_the_table_and_hand_on_the_sdk_calls(monkeypatch):
    import zerobus.sdk.aio as sdk
    from zerobus.sdk.shared import RecordType

    calls = []

    class FakeSdkStream:
        async def ingest_records_offset(self, payloads):
            calls.append(("ingest", list(payloads)))
            return 41

        async def wait_for_offset(self, offset):
            calls.append(("wait", offset))

        async def close(self):
            calls.append(("close",))

    class FakeSdk:
        def __init__(self, host, unity_catalog_url, application_name):
            calls.append(("sdk", host, unity_catalog_url))

        async def create_stream(self, client_id, client_secret, table_properties, options):
            calls.append(("create", client_id, client_secret, table_properties.table_name, options.record_type))
            return FakeSdkStream()

    monkeypatch.setattr(sdk, "ZerobusSdk", FakeSdk)
    streams = ZerobusRowStreams("https://example.zerobus", "https://example.cloud.databricks.com", "c.s.sparkplug_raw",
                                "id", "secret")
    stream = await streams.open()
    assert await stream.ingest(['{"msg_id":"a"}', '{"msg_id":"b"}']) == 41
    await stream.wait_for(41)
    await stream.close()
    assert calls == [("sdk", "https://example.zerobus", "https://example.cloud.databricks.com"),
                     ("create", "id", "secret", "c.s.sparkplug_raw", RecordType.JSON),
                     ("ingest", ['{"msg_id":"a"}', '{"msg_id":"b"}']), ("wait", 41), ("close",)]


def test_the_sdk_has_the_calls_zerobus_row_streams_makes():
    """The fake above stands in for these: the installed SDK (requirements.txt) must take the same arguments."""
    from zerobus.sdk.aio import ZerobusSdk, ZerobusStream

    def params(f):
        return list(inspect.signature(f).parameters)

    assert params(ZerobusSdk.__init__)[1:4] == ["host", "unity_catalog_url", "application_name"]
    assert params(ZerobusSdk.create_stream)[1:5] == ["client_id", "client_secret", "table_properties", "options"]
    assert params(ZerobusStream.ingest_records_offset)[1:] == ["payloads"]
    assert params(ZerobusStream.wait_for_offset)[1:] == ["offset"]
    for f in (ZerobusSdk.create_stream, ZerobusStream.ingest_records_offset, ZerobusStream.wait_for_offset,
              ZerobusStream.close):
        assert inspect.iscoroutinefunction(f), f.__qualname__


# ----------------------------------------------------------------------------- the bridge with confirmed writes

async def test_bridge_writes_sparkplug_rows_through_the_confirmed_writer(monkeypatch):
    streams = FakeStreams()

    class FakeRowStreams:
        def __init__(self, *args):
            self.args = args

        async def open(self):
            return await streams.open()

    monkeypatch.setattr(core_bridge, "ZerobusRowStreams", FakeRowStreams)
    bridge = core_bridge.Bridge({
        "sources": [{"name": "sp", "protocol": "sparkplug", "endpoint": "mqtt://127.0.0.1:1", "enabled": True}],
        "zerobus": {"enabled": True, "zerobus_endpoint": "https://example.zerobus",
                    "auth": {"client_id": "id", "client_secret": "secret"},
                    "default_target": {"workspace_host": "https://example.cloud.databricks.com", "catalog": "c",
                                       "schema": "s", "table": "sparkplug_raw"}},
        "backpressure": {},
    })
    await bridge.start_infra()
    try:
        ndeath = (FIXTURES / "transmission" / "ndeath-1.bin").read_bytes()
        rows = [convert("spBv1.0/Plant1/NDEATH/Line A", ndeath, 1790694245123 + i).row for i in range(3)]
        for r in rows:
            bridge._on_record(r)
        writer = next(iter(bridge.writers.values()))
    finally:
        await bridge.stop()                               # drains: everything queued is written first
    assert writer.status().written == 3
    sent = [json.loads(j) for batch in streams.ingested for j in batch]
    assert [tuple(s) for s in sent] == [COLUMNS] * 3
    assert [s["msg_id"] for s in sent] == [r.msg_id for r in rows]
    metrics = bridge.get_metrics()["zerobus_targets"]
    assert next(iter(metrics.values()))["written"] == 3
