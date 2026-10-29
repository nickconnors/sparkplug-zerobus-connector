"""The Sparkplug primary host against a real Mosquitto, and the bridge going OFFLINE (and disconnecting)
while its confirmed writes fail, ONLINE on a new connection once they recover.

Needs Docker and the ``eclipse-mosquitto:2`` image (skipped otherwise): retained STATE and the will must
behave as on a real MQTT server, which the in-process amqtt broker doesn't guarantee.
"""
from __future__ import annotations

import asyncio
import json
import logging
import shutil
import socket
import subprocess
import time

import aiomqtt
import pytest

from test_confirmed_writer import FakeStreams
from test_sparkplug_source import transmission_messages, wait_for
from sparkplug_connector.core import bridge as core_bridge
from sparkplug_connector.protocols.sparkplug_client import SparkplugClient

HOST_ID = "test-host"
STATE_V3 = f"spBv1.0/STATE/{HOST_ID}"
IMAGE = "eclipse-mosquitto:2"


@pytest.fixture(scope="module")
def mosquitto():
    if shutil.which("docker") is None or subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        pytest.skip("Docker not available")
    if subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True).returncode != 0:
        pytest.skip(f"{IMAGE} image not available locally")
    cid = subprocess.run(["docker", "run", "-d", "--rm", "-p", "127.0.0.1::1883", IMAGE, "mosquitto", "-c", "/mosquitto-no-auth.conf"],
                         capture_output=True, text=True, check=True).stdout.strip()
    try:
        port = int(subprocess.run(["docker", "port", cid, "1883/tcp"], capture_output=True, text=True, check=True)
                   .stdout.strip().splitlines()[0].rsplit(":", 1)[1])
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                break
            except OSError:
                time.sleep(0.1)
        yield port
    finally:
        subprocess.run(["docker", "rm", "-f", cid], capture_output=True)


async def retained(port: int, topic: str, timeout: float = 3.0) -> bytes | None:
    """The retained message on ``topic`` (None if there is none)."""
    async with aiomqtt.Client("127.0.0.1", port, identifier=f"reader-{time.monotonic_ns()}") as c:
        await c.subscribe(topic, qos=1)
        try:
            async with asyncio.timeout(timeout):
                async for m in c.messages:
                    return bytes(m.payload)
        except TimeoutError:
            return None


async def await_retained(port: int, topic: str, predicate, timeout: float = 10.0) -> bytes:
    deadline = time.monotonic() + timeout
    while True:
        value = await retained(port, topic, timeout=1.0)
        if value is not None and predicate(value):
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"retained {topic}: {value!r}")
        await asyncio.sleep(0.1)


def host(port: int, **config) -> tuple[SparkplugClient, list]:
    rows: list = []
    c = SparkplugClient("chariot", f"mqtt://127.0.0.1:{port}", {"client_id": f"host-{time.monotonic_ns()}",
                                                              "primary_host_id": HOST_ID, **config}, on_record=rows.append)
    c.online_allowed.set()          # no supervisor here: allowed ONLINE
    return c, rows


async def online(c: SparkplugClient) -> asyncio.Task:
    await c.connect()
    task = asyncio.create_task(c.subscribe())
    await asyncio.wait_for(c.subscribed.wait(), 5)
    return task


async def offline(c: SparkplugClient, task: asyncio.Task) -> None:
    await c.go_offline()
    await asyncio.gather(task, return_exceptions=True)
    await c.close()


async def test_online_state_is_retained_and_carries_the_will_timestamp(mosquitto):
    c, _ = host(mosquitto)
    task = await online(c)
    try:
        state = json.loads(await await_retained(mosquitto, STATE_V3, lambda v: json.loads(v)["online"]))
        assert state == {"online": True, "timestamp": c.session_timestamp} and c.is_online
    finally:
        await offline(c, task)


async def test_a_crash_fires_the_offline_will_with_the_same_timestamp(mosquitto):
    c, _ = host(mosquitto)
    task = await online(c)
    t = c.session_timestamp
    c._client._client.socket().shutdown(socket.SHUT_RDWR)          # gone without DISCONNECT: the broker sends the will
    state = json.loads(await await_retained(mosquitto, STATE_V3, lambda v: not json.loads(v)["online"]))
    assert state == {"online": False, "timestamp": t}
    await c.disconnect()
    await asyncio.gather(task, return_exceptions=True)


async def test_offline_then_online_again_uses_a_later_timestamp(mosquitto):
    c, _ = host(mosquitto)
    task = await online(c)
    t1 = c.session_timestamp
    await offline(c, task)
    off = json.loads(await await_retained(mosquitto, STATE_V3, lambda v: not json.loads(v)["online"]))
    assert off["timestamp"] >= t1 and not c.is_connected
    await asyncio.sleep(0.01)
    task = await online(c)
    try:
        on = json.loads(await await_retained(mosquitto, STATE_V3, lambda v: json.loads(v)["online"]))
        assert on["timestamp"] == c.session_timestamp > t1 and on["timestamp"] >= off["timestamp"]
    finally:
        await offline(c, task)


async def test_republishes_online_when_its_own_state_is_set_offline(mosquitto):
    c, _ = host(mosquitto)
    task = await online(c)
    try:
        t = c.session_timestamp
        await wait_for(lambda: c.counters["messages_received"] >= 1)      # its own ONLINE came back
        async with aiomqtt.Client("127.0.0.1", mosquitto, identifier="stray") as stray:
            await stray.publish(STATE_V3, json.dumps({"online": False, "timestamp": t + 1}), qos=1, retain=True)
        await wait_for(lambda: c.counters["state_republished"] == 1)
        state = json.loads(await await_retained(mosquitto, STATE_V3, lambda v: json.loads(v)["online"]))
        assert state == {"online": True, "timestamp": t}
    finally:
        await offline(c, task)


async def test_subscribes_to_its_own_state_topic_even_under_the_namespace_filter(mosquitto, monkeypatch):
    subscribed: list[str] = []
    original = aiomqtt.Client.subscribe

    async def recording(self, topic, *args, **kwargs):
        subscribed.append(topic)
        return await original(self, topic, *args, **kwargs)
    monkeypatch.setattr(aiomqtt.Client, "subscribe", recording)
    c, _ = host(mosquitto)
    task = await online(c)
    try:
        assert subscribed == ["spBv1.0/#", STATE_V3]           # the Sparkplug TCK wants the STATE filter itself
    finally:
        await offline(c, task)


async def test_an_offline_delivered_once_per_filter_gets_one_online(mosquitto):
    """Mosquitto sends a message once per matching subscription: the OFFLINE comes twice (spBv1.0/# and the STATE
    topic), ONLINE goes out once."""
    c, _ = host(mosquitto)
    task = await online(c)
    published: list[dict] = []
    try:
        t = c.session_timestamp
        async with aiomqtt.Client("127.0.0.1", mosquitto, identifier="watcher") as watcher:
            await watcher.subscribe(STATE_V3, qos=1)

            async def watch():
                async for m in watcher.messages:
                    if not m.retain:
                        published.append(json.loads(m.payload))
            watching = asyncio.create_task(watch())
            await wait_for(lambda: c.counters["messages_received"] >= 1)    # its own ONLINE (twice: counted once)
            await asyncio.sleep(0.3)
            before = c.counters["messages_received"]
            async with aiomqtt.Client("127.0.0.1", mosquitto, identifier="stray") as stray:
                await stray.publish(STATE_V3, json.dumps({"online": False, "timestamp": t + 1}), qos=1, retain=True)
            await wait_for(lambda: c.counters["messages_received"] >= before + 2)    # the OFFLINE and the new ONLINE
            await asyncio.sleep(0.5)
            watching.cancel()
        assert c.counters["state_republished"] == 1 and c.counters["messages_received"] == before + 2   # copies not counted
        assert published == [{"online": False, "timestamp": t + 1}, {"online": True, "timestamp": t}]
    finally:
        await offline(c, task)


async def test_the_same_offline_again_is_answered_again_when_the_online_wasnt_published(mosquitto, monkeypatch):
    c, _ = host(mosquitto)
    task = await online(c)
    try:
        t = c.session_timestamp
        await wait_for(lambda: c.counters["messages_received"] >= 1)
        original = c._client.publish
        fail = {"n": 1}

        async def flaky(*args, **kwargs):            # the first re-publish of ONLINE fails
            if fail["n"]:
                fail["n"] -= 1
                raise aiomqtt.MqttError("publish failed")
            return await original(*args, **kwargs)
        monkeypatch.setattr(c._client, "publish", flaky)
        offline_state = json.dumps({"online": False, "timestamp": t + 1})
        async with aiomqtt.Client("127.0.0.1", mosquitto, identifier="stray") as stray:
            await stray.publish(STATE_V3, offline_state, qos=1, retain=True)
            await wait_for(lambda: fail["n"] == 0)
            await asyncio.sleep(0.3)
            await stray.publish(STATE_V3, offline_state, qos=1, retain=True)      # the same bytes again
        await wait_for(lambda: c.counters["state_republished"] == 1)
        state = json.loads(await await_retained(mosquitto, STATE_V3, lambda v: json.loads(v)["online"]))
        assert state == {"online": True, "timestamp": t}
    finally:
        await offline(c, task)


async def test_the_previous_sessions_retained_offline_is_not_answered(mosquitto):
    c, _ = host(mosquitto)
    task = await online(c)
    await offline(c, task)                                           # leaves OFFLINE retained
    task = await online(c)
    try:
        await asyncio.sleep(0.5)
        assert c.counters["state_republished"] == 0
    finally:
        await offline(c, task)


async def test_legacy_state_format(mosquitto):
    c, _ = host(mosquitto, state_format="v2")
    task = await online(c)
    assert await await_retained(mosquitto, f"STATE/{HOST_ID}", lambda v: v == b"ONLINE") == b"ONLINE"
    await offline(c, task)
    assert await await_retained(mosquitto, f"STATE/{HOST_ID}", lambda v: v == b"OFFLINE") == b"OFFLINE"


async def test_sparkplug_messages_become_rows(mosquitto):
    c, rows = host(mosquitto, sequence_handling="keep_all")     # a DBIRTH alone: no row in strict
    task = await online(c)
    try:
        topic, payload, java = next(m for m in transmission_messages() if "/DBIRTH/" in m[0])
        async with aiomqtt.Client("127.0.0.1", mosquitto, identifier="transmission") as pub:
            await pub.publish(topic, payload, qos=0)
        await wait_for(lambda: len(rows) == 1)
        assert rows[0].message_type == "DBIRTH" and rows[0].device_id == "M100" and rows[0].msg == java
    finally:
        await offline(c, task)


async def test_as_primary_host_too_an_edge_node_in_mid_session_is_asked_for_a_rebirth(mosquitto, monkeypatch):
    from sparkplug_connector.protocols import sparkplug_client
    from sparkplug_connector.sparkplug import sparkplug_b_pb2 as pb

    monkeypatch.setattr(sparkplug_client, "TICK_S", 0.05)
    c, rows = host(mosquitto, sequence_reordering_timeout=200)
    task = await online(c)
    ncmds: list = []
    try:
        async with aiomqtt.Client("127.0.0.1", mosquitto, identifier="edge") as edge:
            await edge.subscribe("spBv1.0/Plant1/NCMD/#")

            async def listen():
                async for m in edge.messages:
                    ncmds.append(m.topic.value)
            listener = asyncio.create_task(listen())
            await edge.publish("spBv1.0/Plant1/DDATA/Line A/M100", pb.Payload(timestamp=1, seq=42).SerializeToString())
            await wait_for(lambda: ncmds, 5)
            listener.cancel()
        assert ncmds == ["spBv1.0/Plant1/NCMD/Line A"] and not rows and c.is_online
    finally:
        await offline(c, task)


# ----------------------------------------------------------------------------- the bridge: OFFLINE while writes fail

async def test_the_bridge_goes_offline_while_writes_fail_and_online_again_after_they_recover(mosquitto, monkeypatch, caplog):
    streams = FakeStreams()

    class FakeRowStreams:
        def __init__(self, *args):
            pass

        async def open(self):
            return await streams.open()

    monkeypatch.setattr(core_bridge, "ZerobusRowStreams", FakeRowStreams)
    bridge = core_bridge.Bridge({
        "sources": [{"name": "chariot", "protocol": "sparkplug", "endpoint": f"mqtt://127.0.0.1:{mosquitto}",
                     "client_id": "d1-test", "primary_host_id": HOST_ID, "offline_after_seconds": 2, "online_after_seconds": 2,
                     "sequence_handling": "keep_all"}],                 # rows for messages without a birth
        "zerobus": {"enabled": True, "zerobus_endpoint": "https://example.zerobus",
                    "auth": {"client_id": "id", "client_secret": "secret"},
                    "default_target": {"workspace_host": "https://example.cloud.databricks.com", "catalog": "c", "schema": "s",
                                       "table": "raw_messages"}},
        "backpressure": {},
    })
    topic, payload, _ = transmission_messages()[0]
    await bridge.start()
    try:
        await wait_for(lambda: "chariot" in bridge.clients)
        client = bridge.clients["chariot"]
        await wait_for(lambda: client.is_online, 10)                 # ONLINE once the first stream opened
        t1 = client.session_timestamp
        streams.fail_waits = 10 ** 6                                 # Databricks stops acknowledging
        async with aiomqtt.Client("127.0.0.1", mosquitto, identifier="transmission") as pub:
            await pub.publish(topic, payload, qos=0)
        await wait_for(lambda: not client.is_connected, 15)          # OFFLINE after offline_after_seconds
        off = json.loads(await await_retained(mosquitto, STATE_V3, lambda v: not json.loads(v)["online"]))
        assert off["timestamp"] >= t1 and bridge.host_supervisors["chariot"].state.mode == "OFFLINE"
        streams.fail_waits = 0                                       # Databricks is back: the batch goes through
        await wait_for(lambda: client.is_online, 30)                 # ONLINE after online_after_seconds, new will
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR and "[chariot] Error" in r.getMessage()]
        on = json.loads(await await_retained(mosquitto, STATE_V3, lambda v: json.loads(v)["online"]))
        assert on["timestamp"] == client.session_timestamp > t1
        health = bridge.sparkplug_health()["chariot"]
        assert health["state"] == "ONLINE" and health["rows_written"] == 1 and health["zerobus_failures"] >= 1
    finally:
        await bridge.stop()
    off = json.loads(await await_retained(mosquitto, STATE_V3, lambda v: not json.loads(v)["online"]))
    assert off["timestamp"] >= on["timestamp"]                       # stopping publishes OFFLINE first


# ----------------------------------------------------------------------------- rebirth requests on request

async def test_an_operators_rebirth_request_is_the_ncmd_sent_at_once(mosquitto):
    from sparkplug_connector.sparkplug import sparkplug_b_pb2 as pb

    c, _ = host(mosquitto, sequence_handling="keep_all", block_auto_rebirth=True)   # not automatic: not blocked
    task = await online(c)
    ncmds: list = []
    try:
        async with aiomqtt.Client("127.0.0.1", mosquitto, identifier="edge") as edge:
            await edge.subscribe("spBv1.0/Plant1/NCMD/#")

            async def listen():
                async for m in edge.messages:
                    ncmds.append((m.topic.value, m.qos, m.retain, pb.Payload.FromString(m.payload)))
            listener = asyncio.create_task(listen())
            assert await c.request_rebirth("Plant1", "Line A") == (True, "NCMD sent on spBv1.0/Plant1/NCMD/Line A")
            await wait_for(lambda: ncmds, 5)
            listener.cancel()
        (topic, qos, retain, p), = ncmds
        assert (topic, qos, retain, p.HasField("seq")) == ("spBv1.0/Plant1/NCMD/Line A", 0, False, False)
        assert [(m.name, m.datatype, m.boolean_value) for m in p.metrics] == [("Node Control/Rebirth", 11, True)]
        assert c.stats()["rebirth_requests_on_request"] == 1 and c.stats()["rebirth_requests_sent"] == 1
    finally:
        await offline(c, task)


async def test_an_operators_rebirth_request_needs_a_connection_valid_ids_and_unrefused_ncmds(mosquitto):
    c, _ = host(mosquitto)
    assert await c.request_rebirth("Plant1", "Line A") == (False, "not connected")
    task = await online(c)
    try:
        for group, edge in (("Plant1", ""), ("Plant/1", "Line A"), ("Plant1", "Line+"), (None, "Line A")):
            with pytest.raises(ValueError):
                await c.request_rebirth(group, edge)
        c.rebirth_refused = True
        sent, why = await c.request_rebirth("Plant1", "Line A")
        assert not sent and "refused" in why and c.stats()["rebirth_requests_sent"] == 0
    finally:
        await offline(c, task)


async def test_after_an_operators_rebirth_request_no_automatic_one_follows_within_5_s(mosquitto):
    from sparkplug_connector.sparkplug import sparkplug_b_pb2 as pb

    c, _ = host(mosquitto)
    task = await online(c)
    ncmds: list = []
    try:
        async with aiomqtt.Client("127.0.0.1", mosquitto, identifier="edge") as edge:
            await edge.subscribe("spBv1.0/Plant1/NCMD/#")

            async def listen():
                async for m in edge.messages:
                    ncmds.append(m.topic.value)
            listener = asyncio.create_task(listen())
            assert (await c.request_rebirth("Plant1", "Line A"))[0]
            # data from the edge node, never born: an automatic request at once, unless one went out < 5 s ago
            await edge.publish("spBv1.0/Plant1/NDATA/Line A", pb.Payload(timestamp=1, seq=0).SerializeToString())
            await wait_for(lambda: c.stats()["rebirth_requests_rate_limited"] == 1, 5)
            await asyncio.sleep(0.3)
            listener.cancel()
        assert ncmds == ["spBv1.0/Plant1/NCMD/Line A"]
    finally:
        await offline(c, task)


# ----------------------------------------------------------------------------- the bridge's log at a stop

def _bridge_with(mosquitto, monkeypatch, **source):
    streams = FakeStreams()

    class FakeRowStreams:
        def __init__(self, *args):
            pass

        async def open(self):
            return await streams.open()

    monkeypatch.setattr(core_bridge, "ZerobusRowStreams", FakeRowStreams)
    return core_bridge.Bridge({
        "sources": [{"name": "chariot", "protocol": "sparkplug", "endpoint": f"mqtt://127.0.0.1:{mosquitto}",
                     "client_id": f"log-{time.monotonic_ns()}", **source}],
        "zerobus": {"enabled": True, "zerobus_endpoint": "https://example.zerobus",
                    "auth": {"client_id": "id", "client_secret": "secret"},
                    "default_target": {"workspace_host": "https://example.cloud.databricks.com", "catalog": "c", "schema": "s",
                                       "table": "raw_messages"}},
        "backpressure": {},
    })


def _bridge_warnings(caplog):
    return [r.getMessage() for r in caplog.records if r.name == core_bridge.__name__ and r.levelno >= logging.WARNING]


@pytest.mark.parametrize("primary", [True, False])
async def test_stopping_the_bridge_or_a_source_logs_no_error(mosquitto, monkeypatch, caplog, primary):
    caplog.set_level(logging.INFO)
    bridge = _bridge_with(mosquitto, monkeypatch, **({"primary_host_id": HOST_ID} if primary else {}))
    await bridge.start()
    try:
        await wait_for(lambda: "chariot" in bridge.clients and bridge.clients["chariot"].subscribed.is_set(), 10)
        assert (await bridge.stop_source("chariot"))["status"] == "ok"
        await bridge.start_source("chariot")
        await wait_for(lambda: "chariot" in bridge.clients and bridge.clients["chariot"].subscribed.is_set(), 10)
    finally:
        await bridge.stop()
    assert _bridge_warnings(caplog) == []
    assert any("subscription ended: the source is stopping" in r.getMessage() for r in caplog.records)


async def test_a_connection_lost_is_still_an_error(mosquitto, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    bridge = _bridge_with(mosquitto, monkeypatch)
    await bridge.start()
    try:
        await wait_for(lambda: "chariot" in bridge.clients and bridge.clients["chariot"].subscribed.is_set(), 10)
        bridge.clients["chariot"]._client._client.socket().shutdown(socket.SHUT_RDWR)
        await wait_for(lambda: any(r.levelno == logging.ERROR and "[chariot] Error" in r.getMessage()
                                   for r in caplog.records), 10)
    finally:
        await bridge.stop()
