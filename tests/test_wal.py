"""The write-ahead log (core/wal.py) and the bridge with zerobus.wal: rows not acknowledged survive a crash."""
from __future__ import annotations

import asyncio
import errno
import json
import os

import pytest

from test_confirmed_writer import FakeStreams, row
from test_per_target_queues import rows, wait_for
from sparkplug_connector.core import bridge as core_bridge, wal as wal_module
from sparkplug_connector.core.wal import SEQ_FIELD, WriteAheadLog


def ids(records):
    return [r["msg_id"] for r in records]


def segments(directory):
    return sorted(p.name for p in directory.glob("wal-*.log"))


# ----------------------------------------------------------------------------- the log

def test_rows_not_done_come_back_in_order_with_their_sequence(tmp_path):
    log = WriteAheadLog(tmp_path)
    assert log.open() == []
    assert [log.append(row(f"m{i}")) for i in range(3)] == [1, 2, 3]
    log.close()                                            # a crash: no done() for any of them

    again = WriteAheadLog(tmp_path)
    pending = again.open()
    assert ids(pending) == ["m0", "m1", "m2"]
    assert [r[SEQ_FIELD] for r in pending] == [1, 2, 3]
    assert pending[0] == {**row("m0"), SEQ_FIELD: 1}       # the record as appended
    assert again.status().rows_replayed == 3 and again.status().pending == 3
    assert again.append(row("m3")) == 4                    # sequences carry on
    again.close()


def test_done_rows_are_not_replayed_and_their_segments_go(tmp_path):
    log = WriteAheadLog(tmp_path)
    log.open()
    for i in range(3):
        log.append(row(f"m{i}"))
    log.done([1, 2, 3])
    assert (tmp_path / "checkpoint").read_text() == "3"
    log.close()
    assert len(segments(tmp_path)) == 1                    # the last segment stays until the next start

    again = WriteAheadLog(tmp_path)
    assert again.open() == []
    assert segments(tmp_path) == [] and again.status().checkpoint == 3
    assert again.append(row("m3")) == 4
    again.close()


def test_the_checkpoint_waits_for_rows_done_out_of_order(tmp_path):
    """A row the queue refuses (drop_policy newest or reject) is done before the rows queued ahead of it: the
    checkpoint only passes rows with nothing
    outstanding before them; rows done above it are written again after a crash (at least once)."""
    log = WriteAheadLog(tmp_path)
    log.open()
    for i in range(5):
        log.append(row(f"m{i}"))
    log.done([2, 3, 5])
    assert log.status().checkpoint == 0 and log.status().pending == 2
    log.done([1])
    assert log.status().checkpoint == 3
    log.close()
    assert [r[SEQ_FIELD] for r in WriteAheadLog(tmp_path).open()] == [4, 5]


def test_a_torn_last_line_and_a_damaged_line_are_skipped_and_counted(tmp_path):
    log = WriteAheadLog(tmp_path)
    log.open()
    for i in range(3):
        log.append(row(f"m{i}"))
    log.close()
    path = tmp_path / segments(tmp_path)[0]
    lines = path.read_bytes().split(b"\n")
    lines[1] = lines[1].replace(b'"m1"', b'"mX"')          # checksum no longer matches
    path.write_bytes(b"\n".join(lines) + b'4 0000abcd {"msg_id":"m3"')   # a write cut off by the crash

    again = WriteAheadLog(tmp_path)
    assert ids(again.open()) == ["m0", "m2"]
    assert again.status().corrupt_lines == 2
    again.close()


@pytest.mark.parametrize("content", [b"", b'2 0000abcd {"msg_id":"m1'], ids=["created, never written", "a torn line"])
def test_a_newest_segment_without_a_good_line_is_not_reused(tmp_path, content):
    log = WriteAheadLog(tmp_path)
    log.open()
    log.append(row("m0"))                                  # not acknowledged
    log.close()
    (tmp_path / f"wal-{2:020d}.log").write_bytes(content)   # the crash: right after the next segment's start

    again = WriteAheadLog(tmp_path)
    assert ids(again.open()) == ["m0"]
    assert again.append(row("m1")) == 2                    # a segment named for sequence 2 again
    again.done([1])                                        # m0 written: the checkpoint passes the old wal-2
    again.close()                                          # another crash, m1 not acknowledged
    # it was glued to the torn line, and its file deleted with the stale entry
    assert ids(WriteAheadLog(tmp_path).open()) == ["m1"]


def test_segments_rotate_and_are_deleted_once_done(tmp_path):
    log = WriteAheadLog(tmp_path, segment_bytes=1)          # one row per segment
    log.open()
    for i in range(6):
        log.append(row(f"m{i}"))
    assert len(segments(tmp_path)) == 6
    log.done([1, 2, 3])
    assert len(segments(tmp_path)) == 3 and log.status().segments == 3
    log.done([4, 5, 6])
    assert len(segments(tmp_path)) == 1                    # the current one
    log.close()


def test_a_full_log_queues_rows_without_it_until_space_is_freed(tmp_path):
    line = len(b"1 00000000 " + json.dumps(row("m0"), separators=(",", ":")).encode() + b"\n")
    log = WriteAheadLog(tmp_path, segment_bytes=1, max_bytes=2 * line)
    log.open()
    assert log.append(row("m0")) == 1 and log.append(row("m1")) == 2
    assert log.append(row("m2")) is None
    assert log.status().rows_not_logged == 1
    log.done([1, 2])                                       # the first segment goes
    assert log.append(row("m3")) == 3
    log.close()


def test_a_row_the_disk_cant_take_is_only_queued_and_its_part_line_is_cut_off(tmp_path, monkeypatch):
    """A full disk takes part of the line, then write() fails: the row is queued without the log, and the
    next row's line doesn't follow the part written (it would be unreadable at the next start)."""
    log = WriteAheadLog(tmp_path)
    log.open()
    assert log.append(row("m0")) == 1
    write, calls = os.write, []

    def disk_full(fd, data):
        calls.append(len(data))
        if len(calls) == 1:
            return write(fd, data[:len(data) // 2])        # what fits
        raise OSError(errno.ENOSPC, "No space left on device")
    monkeypatch.setattr(wal_module.os, "write", disk_full)
    assert log.append(row("m1")) is None
    monkeypatch.setattr(wal_module.os, "write", write)
    status = log.status()
    assert (status.rows_not_logged, status.failures, status.pending) == (1, 1, 1)
    assert "No space left on device" in status.last_error
    assert log.append(row("m2")) == 2
    log.close()

    again = WriteAheadLog(tmp_path)
    assert ids(again.open()) == ["m0", "m2"] and again.status().corrupt_lines == 0
    again.close()


async def test_appends_are_fsynced_on_a_timer(tmp_path, monkeypatch):
    synced = []
    monkeypatch.setattr(wal_module.os, "fsync", lambda fd: synced.append(fd))
    log = WriteAheadLog(tmp_path, fsync_interval_s=0.01)
    log.open()
    task = asyncio.create_task(log.run())
    try:
        log.append(row("m0"))
        await wait_for(lambda: len(synced) >= 2)            # the segment and the directory
        count = len(synced)
        await asyncio.sleep(0.05)
        assert len(synced) == count                        # nothing new: no fsync
    finally:
        task.cancel()
    log.close()


# ----------------------------------------------------------------------------- the bridge

def bridge_config(tmp_path, max_queue: int = 1000) -> dict:
    """One enabled Sparkplug source on the default table (its table is opened; start_infra() starts no source,
    and nothing listens at its endpoint)."""
    return {
        "sources": [{"name": "sp", "protocol": "sparkplug", "endpoint": "mqtt://127.0.0.1:1", "enabled": True}],
        "zerobus": {"enabled": True, "zerobus_endpoint": "https://example.zerobus",
                    "auth": {"client_id": "id", "client_secret": "secret"},
                    "default_target": {"workspace_host": "https://example.cloud.databricks.com", "catalog": "c",
                                       "schema": "s", "table": "sparkplug_raw"},
                    "wal": {"enabled": "true", "path": str(tmp_path / "wal"), "fsync_interval_seconds": 0.05},
                    "batch": {"linger_seconds": 0.02}, "drain_timeout_seconds": 5},
        "backpressure": {"memory_queue": {"max_size": max_queue, "drop_policy": "oldest"}},
    }


@pytest.fixture
def fake_streams(monkeypatch):
    streams = FakeStreams()

    class FakeRowStreams:
        def __init__(self, *args):
            pass

        async def open(self):
            return await streams.open()

    monkeypatch.setattr(core_bridge, "ZerobusRowStreams", FakeRowStreams)
    return streams


async def crash(bridge):
    """Stops the bridge's tasks without its stop(): nothing drained, no OFFLINE, logs only closed."""
    tasks = [*bridge.writer_tasks.values(), *bridge.wal_tasks.values(), *bridge.host_tasks.values()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for log in bridge.wals.values():
        log.close()


async def test_rows_queued_when_the_process_dies_are_written_after_the_restart(tmp_path, fake_streams):
    fake_streams.fail_opens = 10**6                       # Databricks unreachable: everything stays queued
    first = core_bridge.Bridge(bridge_config(tmp_path))
    await first.start_infra()
    sent = rows("A", 4)
    for r in sent:
        first._on_record(r)
    assert first.get_metrics()["wal"]["example.c.s.sparkplug_raw"]["pending"] == 4
    await crash(first)

    fake_streams.fail_opens = 0
    second = core_bridge.Bridge(bridge_config(tmp_path))
    await second.start_infra()
    later = rows("A", 1)
    second._on_record(later[0])
    await second.stop()
    written = [json.loads(j) for batch in fake_streams.ingested for j in batch]
    assert ids(written) == [r.msg_id for r in sent + later]   # the replayed rows first, same msg_id
    assert all(SEQ_FIELD not in w for w in written)
    status = second.get_metrics()["wal"]["example.c.s.sparkplug_raw"]
    assert status["rows_replayed"] == 4 and status["pending"] == 0 and status["checkpoint"] == 5

    batches = len(fake_streams.ingested)
    third = core_bridge.Bridge(bridge_config(tmp_path))      # after a clean stop: nothing to replay
    await third.start_infra()
    await third.stop()
    assert third.get_metrics()["wal"]["example.c.s.sparkplug_raw"]["rows_replayed"] == 0
    assert len(fake_streams.ingested) == batches
    assert segments(tmp_path / "wal" / "example.c.s.sparkplug_raw") == []


async def test_a_batch_in_flight_when_the_process_dies_is_written_again(tmp_path, fake_streams):
    fake_streams.fail_waits = 10**6                       # sent, never acknowledged
    first = core_bridge.Bridge(bridge_config(tmp_path))
    await first.start_infra()
    sent = rows("A", 3)
    for r in sent:
        first._on_record(r)
    await wait_for(lambda: fake_streams.ingested)
    await crash(first)

    fake_streams.fail_waits, fake_streams.ingested = 0, []
    second = core_bridge.Bridge(bridge_config(tmp_path))
    await second.start_infra()
    await second.stop()
    assert ids(json.loads(j) for batch in fake_streams.ingested for j in batch) == [r.msg_id for r in sent]


@pytest.mark.parametrize("drop_policy", ["oldest", "reject"])
async def test_more_rows_to_replay_than_the_queue_holds_are_all_written_first(tmp_path, fake_streams, drop_policy):
    log = WriteAheadLog(tmp_path / "wal" / "example.c.s.sparkplug_raw")
    log.open()
    sent = rows("A", 10)
    for r in sent:                                          # as the bridge logs a Sparkplug row
        log.append(r.to_zerobus_record())
    log.close()                                            # a crash with 10 rows not acknowledged

    cfg = bridge_config(tmp_path, max_queue=3)
    cfg["backpressure"]["memory_queue"]["drop_policy"] = drop_policy
    bridge = core_bridge.Bridge(cfg)
    await bridge.start_infra()
    assert "example.c.s.sparkplug_raw" in bridge.writers   # set up despite the small queue
    later = rows("A", 1)
    bridge._on_record(later[0])
    await bridge.stop()
    written = ids(json.loads(j) for batch in fake_streams.ingested for j in batch)
    assert written == [r.msg_id for r in sent + later]     # none dropped, the replayed rows first
    status = bridge.get_metrics()["wal"]["example.c.s.sparkplug_raw"]
    assert status["pending"] == 0 and status["checkpoint"] == 11
    assert bridge.get_metrics()["backpressure"]["records_dropped"] == 0


async def test_a_row_the_log_cant_write_is_still_queued_and_written(tmp_path, fake_streams, monkeypatch):
    bridge = core_bridge.Bridge(bridge_config(tmp_path))
    await bridge.start_infra()
    sent = rows("A", 3)
    bridge._on_record(sent[0])

    def disk_full(fd, data):
        raise OSError(errno.ENOSPC, "No space left on device")
    with monkeypatch.context() as m:
        m.setattr(wal_module.os, "write", disk_full)
        bridge._on_record(sent[1])                         # nothing raised into the source's message loop
    bridge._on_record(sent[2])
    await bridge.stop()
    assert ids(json.loads(j) for batch in fake_streams.ingested for j in batch) == [r.msg_id for r in sent]
    status = bridge.get_metrics()["wal"]["example.c.s.sparkplug_raw"]
    assert (status["rows_not_logged"], status["pending"], status["checkpoint"]) == (1, 0, 2)


async def test_rows_of_a_take_cancelled_part_way_are_written_by_the_next_writer(tmp_path, fake_streams):
    """The writer task cancelled during a take (e.g. stop_zerobus after its wait): the
    rows it had taken from the queue were lost, and done in the log with the next writer's acknowledgement."""
    bridge = core_bridge.Bridge(bridge_config(tmp_path))
    target = "example.c.s.sparkplug_raw"
    queue = bridge._queue(target)
    real, one_taken = queue.dequeue, asyncio.Event()

    async def slow_after_the_first():
        if one_taken.is_set():
            await asyncio.Event().wait()                    # a slow take: cancelled here
        record = await real()
        if record is not None:
            one_taken.set()
        return record
    queue.dequeue = slow_after_the_first
    await bridge.start_infra()
    sent = rows("A", 1)
    bridge._on_record(sent[0])
    await asyncio.wait_for(one_taken.wait(), 5)
    task = bridge.writer_tasks[target]
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    queue.dequeue = real
    await bridge.stop_zerobus()
    await bridge.start_zerobus()
    await bridge.stop()
    assert ids(json.loads(j) for batch in fake_streams.ingested for j in batch) == [r.msg_id for r in sent]
    assert bridge.get_metrics()["wal"][target]["pending"] == 0

async def test_rows_the_queue_drops_are_done_in_the_log(tmp_path, fake_streams):
    fake_streams.fail_opens = 10**6
    bridge = core_bridge.Bridge(bridge_config(tmp_path, max_queue=2))
    await bridge.start_infra()
    for r in rows("A", 5):
        bridge._on_record(r)
    log = bridge.wals["example.c.s.sparkplug_raw"]
    await wait_for(lambda: log.status().checkpoint == 3)   # 1-3 dropped (oldest first), 4-5 queued
    assert log.status().pending == 2
    await crash(bridge)


async def test_a_row_the_queue_fails_to_take_is_dropped_and_done_in_the_log(tmp_path, fake_streams):
    """The slow path's enqueue failing unexpectedly: the error left the enqueue task, so the row was lost and
    outstanding in the log for good (the checkpoint stopped there: every later row replayed at each start)."""
    fake_streams.fail_opens = 10**6                       # nothing drains: the memory queue stays full
    bridge = core_bridge.Bridge(bridge_config(tmp_path, max_queue=1))
    queue = bridge._queue("example.c.s.sparkplug_raw")

    async def broken(record):
        raise OSError(errno.ENOSPC, "No space left on device")
    queue.enqueue = broken
    await bridge.start_infra()
    try:
        first, second = rows("A", 2)
        bridge._on_record(first)
        bridge._on_record(second)                          # the memory queue is full: the slow path
        await wait_for(lambda: bridge.metrics["records_dropped"] == 1)
        assert bridge.wals["example.c.s.sparkplug_raw"].status().pending == 1
    finally:
        await crash(bridge)

async def test_rows_the_queue_rejects_are_done_in_the_log(tmp_path, fake_streams):
    """drop_policy reject: the rows refused while the queue is full mustn't hold the checkpoint back (every
    later row would be replayed after each restart)."""
    fake_streams.fail_opens = 2                           # ~3 s of backoff before the first take
    cfg = bridge_config(tmp_path, max_queue=2)
    cfg["backpressure"]["memory_queue"]["drop_policy"] = "reject"
    bridge = core_bridge.Bridge(cfg)
    await bridge.start_infra()
    sent = rows("A", 5)
    for r in sent:
        bridge._on_record(r)
    log = bridge.wals["example.c.s.sparkplug_raw"]
    await wait_for(lambda: bridge.metrics["records_dropped"] == 3)   # 1-2 queued, 3-5 refused
    assert log.status().pending == 2 and log.status().checkpoint == 0
    await wait_for(lambda: log.status().checkpoint == 5, timeout=15)  # 1-2 written: past the refused ones
    assert log.status().pending == 0
    assert ids(json.loads(j) for batch in fake_streams.ingested for j in batch) == [r.msg_id for r in sent[:2]]
    await bridge.stop()


async def test_only_the_enabled_sources_tables_are_opened_each_with_a_log(tmp_path, fake_streams):
    cfg = bridge_config(tmp_path)
    cfg["sources"] = [{"name": "sp", "protocol": "sparkplug", "endpoint": "mqtt://127.0.0.1:1", "enabled": True,
                       "target_table": "c.s.sparkplug_raw_2"},
                      {"name": "off", "protocol": "sparkplug", "endpoint": "mqtt://127.0.0.1:1", "enabled": False}]
    bridge = core_bridge.Bridge(cfg)
    await bridge.start_infra()
    try:
        assert list(bridge.tables) == list(bridge.writers) == list(bridge.wals) == ["example.c.s.sparkplug_raw_2"]
    finally:
        await bridge.stop()
