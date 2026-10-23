"""The Sparkplug source's rows and rebirth requests (sparkplug/sequencer.py), through an MQTT broker.

amqtt in-process; a proxy in front of it stands in for Chariot's answer to a publish the ACL refuses (it closes the
connection). The reorder timer ticks every 50 ms here (1 s in the connector).
"""
from __future__ import annotations

import asyncio
import logging
import time

import aiomqtt
import pytest

from sparkplug_connector.core.bridge import Bridge
from sparkplug_connector.protocols import sparkplug_client
from sparkplug_connector.protocols.sparkplug_client import SparkplugClient
from sparkplug_connector.sparkplug import sparkplug_b_pb2 as pb
from test_sparkplug_source import broker, free_port, start_broker, stop_broker, wait_for  # noqa: F401 - the fixture

GROUP, EDGE = "Plant1", "Line A"
NODE, DEVICE = f"spBv1.0/{GROUP}/{{}}/{EDGE}", f"spBv1.0/{GROUP}/{{}}/{EDGE}/M100"
NCMD = f"spBv1.0/{GROUP}/NCMD/{EDGE}"


@pytest.fixture(autouse=True)
def fast_ticks(monkeypatch):
    monkeypatch.setattr(sparkplug_client, "TICK_S", 0.05)


def msg(seq=None, bdseq=None):
    p = pb.Payload(timestamp=int(time.time() * 1000))
    if seq is not None:
        p.seq = seq
    if bdseq is not None:
        p.metrics.add(name="bdSeq", datatype=4, long_value=bdseq)
    p.metrics.add(name="Temp", datatype=9, float_value=float(seq or 0))
    return p.SerializeToString()


def session(*data_seqs, edge=EDGE):
    """NBIRTH 0, DBIRTH 1, then DDATA with these seqs."""
    node, device = NODE.replace(EDGE, edge), DEVICE.replace(EDGE, edge)
    return [(node.format("NBIRTH"), msg(0, 0)), (device.format("DBIRTH"), msg(1))] + \
           [(device.format("DDATA"), msg(s)) for s in data_seqs]


class Edge:
    """The edge's side: publishes, and records the NCMDs it receives."""

    def __init__(self, port):
        self.port, self.ncmds, self._task = port, [], None

    async def __aenter__(self):
        self.client = aiomqtt.Client("127.0.0.1", self.port, identifier=f"edge-{time.monotonic_ns()}")
        await self.client.__aenter__()
        await self.client.subscribe(f"spBv1.0/{GROUP}/NCMD/#")
        self._task = asyncio.create_task(self._listen())
        return self

    async def _listen(self):
        async for m in self.client.messages:
            self.ncmds.append((m.topic.value, bytes(m.payload)))

    async def publish(self, messages):
        for topic, payload in messages:
            await self.client.publish(topic, payload, qos=0)

    async def __aexit__(self, *exc):
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        await self.client.__aexit__(None, None, None)


async def running(client: SparkplugClient) -> asyncio.Task:
    await client.connect()
    task = asyncio.create_task(client.subscribe())
    await asyncio.wait_for(client.subscribed.wait(), 5)
    return task


async def stopped(client: SparkplugClient, task: asyncio.Task) -> None:
    await client.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await client.close()


def source(endpoint, rows, **config):
    return SparkplugClient("chariot", endpoint, {"client_id": f"seq-{time.monotonic_ns()}", "sequence_reordering_timeout": 300,
                                                 **config}, on_record=rows.append)


# ----------------------------------------------------------------------------- strict rows and rebirth requests

async def test_a_gap_gets_strict_rows_and_a_rebirth_request(broker):
    endpoint, port = broker
    rows: list = []
    client = source(endpoint, rows)
    task = await running(client)
    try:
        async with Edge(port) as edge:
            await edge.publish(session(2, 4))                       # seq 3 never comes
            await wait_for(lambda: edge.ncmds, 5)
            topic, payload = edge.ncmds[0]
            ncmd = pb.Payload.FromString(payload)
            assert topic == NCMD and not ncmd.HasField("seq")
            assert [(m.name, m.datatype, m.boolean_value) for m in ncmd.metrics] == [("Node Control/Rebirth", 11, True)]
            assert ncmd.metrics[0].timestamp == ncmd.timestamp
        assert [r.message_type for r in rows] == ["NBIRTH", "DBIRTH", "DDATA"]
        s = client.stats()
        assert (s["dropped_gap_timeout"], s["rebirth_requests_sent"], s["messages_held"]) == (1, 1, 0)
    finally:
        await stopped(client, task)


async def test_keep_all_writes_every_message_and_still_asks(broker):
    endpoint, port = broker
    rows: list = []
    client = source(endpoint, rows, sequence_handling="keep_all")
    task = await running(client)
    try:
        async with Edge(port) as edge:
            await edge.publish(session(2, 4))
            await wait_for(lambda: edge.ncmds, 5)
        assert len(rows) == 4 and client.stats()["dropped_gap_timeout"] == 1
    finally:
        await stopped(client, task)


async def test_block_auto_rebirth_sends_no_ncmd(broker):
    endpoint, port = broker
    rows: list = []
    client = source(endpoint, rows, block_auto_rebirth=True, sequence_handling="keep_all")
    task = await running(client)
    try:
        async with Edge(port) as edge:
            await edge.publish(session(2, 4))
            await wait_for(lambda: client.stats()["rebirth_requests_blocked"] == 1, 5)
            await asyncio.sleep(0.3)
            assert not edge.ncmds
    finally:
        await stopped(client, task)


async def test_an_edge_node_already_publishing_is_asked_for_a_rebirth_and_its_birth_comes_through(broker):
    endpoint, port = broker
    rows: list = []
    client = source(endpoint, rows)
    task = await running(client)
    try:
        async with Edge(port) as edge:
            await edge.publish([(DEVICE.format("DDATA"), msg(40)), (DEVICE.format("DDATA"), msg(41))])
            await wait_for(lambda: edge.ncmds, 5)
            assert not rows
            await edge.publish(session(2))                          # the edge rebirths
            await wait_for(lambda: len(rows) == 3)
    finally:
        await stopped(client, task)


async def test_the_timer_runs_across_reconnects_and_ends_with_close(broker):
    endpoint, port = broker
    rows: list = []
    client = source(endpoint, rows)
    task = await running(client)
    ticker = client._ticker
    await client.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    task = await running(client)                                    # the bridge's reconnect
    assert client._ticker is ticker and not ticker.done()
    async with Edge(port) as edge:
        await edge.publish(session(2, 4))
        await wait_for(lambda: len(rows) == 3)
    await stopped(client, task)
    assert ticker.done() and client._ticker is None
    assert client.stats()["dropped_shutdown"] + client.stats()["dropped_gap_timeout"] == 1


# ----------------------------------------------------------------------------- a refused NCMD (Chariot closes the connection)

class RefusingProxy:
    """A TCP proxy to the broker that closes the connection when the client publishes on an NCMD topic."""

    def __init__(self, broker_port):
        self.broker_port, self.port, self.refused = broker_port, free_port(), 0
        self._writers: set = set()

    async def __aenter__(self):
        self.server = await asyncio.start_server(self._client, "127.0.0.1", self.port)
        return self

    async def __aexit__(self, *exc):
        self.server.close()
        for w in self._writers:
            w.close()                                               # or wait_closed waits for the open connections
        await asyncio.wait_for(self.server.wait_closed(), 5)

    async def _client(self, reader, writer):
        up_reader, up_writer = await asyncio.open_connection("127.0.0.1", self.broker_port)
        self._writers.update((writer, up_writer))

        async def down():
            try:
                while data := await up_reader.read(65536):
                    writer.write(data)
                    await writer.drain()
            except (ConnectionError, asyncio.CancelledError):
                pass

        pump = asyncio.create_task(down())
        try:
            while True:
                header = await reader.readexactly(1)
                length, shift, raw = 0, 0, b""
                while True:
                    b = await reader.readexactly(1)
                    raw += b
                    length |= (b[0] & 0x7F) << shift
                    shift += 7
                    if not b[0] & 0x80:
                        break
                body = await reader.readexactly(length)
                if header[0] >> 4 == 3 and b"/NCMD/" in body[2:2 + int.from_bytes(body[:2], "big")]:
                    self.refused += 1
                    break                                           # refused: the connection is closed
                up_writer.write(header + raw + body)
                await up_writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            pump.cancel()
            writer.close()
            up_writer.close()


async def test_refused_ncmds_stop_the_requests_and_the_source_keeps_every_message(broker, caplog):
    _, port = broker
    rows: list = []
    async with RefusingProxy(port) as proxy:
        client = source(f"mqtt://127.0.0.1:{proxy.port}", rows, username="sparkplug-user")
        task = await running(client)
        async with Edge(port) as edge:
            with caplog.at_level(logging.WARNING):
                await edge.publish(session(2, 4))
                await wait_for(lambda: task.done(), 5)              # the connection was closed: a first strike
                assert proxy.refused == 1 and not client.rebirth_refused and "if it happens again" in caplog.text
                await client.stop()
                task = await running(client)                        # the bridge's reconnect
                await edge.publish(session(2, 4, edge="Line B"))    # another edge node (the first: rate-limited)
                await wait_for(lambda: task.done(), 5)              # refused again
            assert proxy.refused == 2 and client.rebirth_refused and client.sequence_handling == "keep_all"
            assert "may not publish on spBv1.0/<group>/NCMD/<edge node>" in caplog.text and "'sparkplug-user'" in caplog.text
            await client.stop()
            task = await running(client)
            before = len(rows)
            await edge.publish(session(2, 4, edge="Line C"))
            await wait_for(lambda: client.stats()["rebirth_requests_not_sent_refused"] == 1, 5)
            assert len(rows) - before == 4                          # keep_all: DDATA 4 too
            assert proxy.refused == 2 and not task.done() and not edge.ncmds
        await stopped(client, task)


def test_messages_between_an_ncmd_and_the_lost_connection_still_count_as_a_refusal():
    """aiomqtt hands over the messages it already had before it reports the lost connection."""
    client = source("mqtt://127.0.0.1:1883", [])
    for strike in (1, 2):
        client._ncmd_at = time.monotonic()
        client.handle_message(DEVICE.format("DDATA"), msg(7), 0)
        client._connection_lost()
    assert client._refusals == 2 and client.rebirth_refused and client.sequence_handling == "keep_all"


def test_a_connection_lost_long_after_an_ncmd_is_not_a_refusal():
    client = source("mqtt://127.0.0.1:1883", [])
    client._ncmd_at = time.monotonic() - sparkplug_client.REFUSED_WITHIN_S - 1
    client._connection_lost()
    assert client._refusals == 0 and not client.rebirth_refused


def test_our_own_disconnect_is_not_a_refusal():
    client = source("mqtt://127.0.0.1:1883", [])
    client._ncmd_at, client._closing = time.monotonic(), True
    client._connection_lost()
    assert client._refusals == 0


async def test_an_ncmd_the_connection_outlives_starts_the_count_again(broker, monkeypatch):
    monkeypatch.setattr(sparkplug_client, "REFUSED_WITHIN_S", 0.1)
    endpoint, port = broker
    client = source(endpoint, [])
    task = await running(client)
    client._refusals = 1                                            # an earlier strike
    async with Edge(port) as edge:
        await edge.publish(session(2, 4))
        await wait_for(lambda: edge.ncmds, 5)
        await wait_for(lambda: client._refusals == 0, 2)
    await stopped(client, task)


# ----------------------------------------------------------------------------- settings and errors

def test_block_auto_rebirth_needs_keep_all():
    with pytest.raises(ValueError, match="needs sequence_handling: keep_all"):
        source("mqtt://127.0.0.1:1883", [], block_auto_rebirth=True)
    assert source("mqtt://127.0.0.1:1883", [], block_auto_rebirth=True, sequence_handling="keep_all").block_auto_rebirth


def test_a_timeout_of_0_is_refused_and_empty_is_the_default():
    with pytest.raises(ValueError):
        source("mqtt://127.0.0.1:1883", [], sequence_reordering_timeout=0)
    assert source("mqtt://127.0.0.1:1883", [], sequence_reordering_timeout="").sequencer.timeout_s == 5.0


@pytest.mark.parametrize("value, on", [(True, True), ("true", True), ("y", True), ("enabled", True), (False, False),
                                       ("false", False), ("off", False), ("0", False), ("", False)])
def test_block_auto_rebirth_reads_true_and_false_as_the_bridge_and_the_web_form_do(value, on):
    from sparkplug_connector.core.bridge import _enabled

    assert sparkplug_client._flag(value) is on is _enabled(value)


def test_a_sequencing_error_keeps_the_row_and_the_subscription(monkeypatch):
    rows: list = []
    client = source("mqtt://127.0.0.1:1883", rows)

    def broken(*args):
        raise RuntimeError("a bug")
    monkeypatch.setattr(client.sequencer, "on_message", broken)
    client.handle_message(DEVICE.format("DDATA"), msg(3), 0)
    assert len(rows) == 1 and client.stats()["sequence_errors"] == 1


# ----------------------------------------------------------------------------- the bridge's health

async def test_the_health_shows_the_sequencing(broker):
    endpoint, port = broker
    bridge = Bridge({"sources": [{"name": "chariot", "protocol": "sparkplug", "endpoint": endpoint, "client_id": "health-test",
                                  "sequence_reordering_timeout": 300}],
                     "zerobus": {"enabled": False}, "backpressure": {}})
    await bridge.start()
    try:
        await wait_for(lambda: "chariot" in bridge.clients and bridge.clients["chariot"].subscribed.is_set())
        async with Edge(port) as edge:
            await edge.publish(session(2, 4))
            await wait_for(lambda: edge.ncmds, 5)
        h = bridge.sparkplug_health()["chariot"]
        assert h["sequence_handling"] == "strict" and h["dropped"] == {"gap_timeout": 1}
        assert (h["rebirth_requests"], h["rebirth_requests_sent"], h["rebirth_refused"]) == (1, 1, False)
        assert (h["rebirth_requests_not_sent_refused"], h["block_auto_rebirth"], h["sequence_errors"]) == (0, False, 0)
    finally:
        await bridge.stop()
