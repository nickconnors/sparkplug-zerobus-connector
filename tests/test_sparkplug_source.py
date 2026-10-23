"""The ``sparkplug`` source: SparkplugClient, and Sparkplug rows through Bridge (Zerobus off).

The end-to-end tests run an in-process MQTT broker (amqtt) and publish the Transmission-shaped
payloads with their real topics; each row's ``msg`` must equal Java Tahu's saved JSON.
"""
from __future__ import annotations

import asyncio
import json
import socket
from collections import Counter

import aiomqtt
import pytest

from conftest import EXPECTED, FIXTURES
from sparkplug_connector.core.bridge import Bridge
from sparkplug_connector.protocols import sparkplug_client
from sparkplug_connector.protocols.factory import create_protocol_client
from sparkplug_connector.protocols.sparkplug_client import SparkplugClient, parse_endpoint
from sparkplug_connector.sparkplug.raw_row import COLUMNS, RawRow, convert

TRANSMISSION = FIXTURES / "transmission"
# a row for every message (the raw-row path); strict (the default): test_sequenced_source.py
KEEP_ALL = {"sequence_handling": "keep_all"}
SKIPPED = [   # (topic, payload, counter): messages that get no row
    ("spBv1.0/STATE/databricks-zerobus", b'{"online":true,"timestamp":1}', "skipped_state"),
    ("STATE/databricks-zerobus", b"ONLINE", "skipped_state"),
    ("spBv1.0/Plant1/NCMD/Line A", b"", "skipped_command"),
    ("spBv1.0/Plant1/DCMD/Line A/M100", b"", "skipped_command"),
    ("spBv1.0/Plant1/DDATA/Line A", b"", "skipped_malformed_topic"),
    ("factory/line1/temperature", b"21.5", "skipped_foreign_topic"),
    ("spBv1.0/Plant1/DDATA/Line A/M100", b"\xff\x01", "skipped_undecodable_payload"),
]


def transmission_messages() -> list[tuple[str, bytes, str]]:
    """(topic, payload, Java Tahu's JSON) for each Transmission-shaped fixture, from topics.tsv."""
    out = []
    for line in (TRANSMISSION / "topics.tsv").read_text().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        name, topic = line.split("\t")[:2]
        stem = name.removesuffix(".bin")
        out.append((topic, (TRANSMISSION / name).read_bytes(), (EXPECTED / f"{stem}.json").read_text(encoding="utf-8")))
    return out


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def start_broker(port: int):
    from amqtt.broker import Broker

    b = Broker({"listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{port}"}}, "sys_interval": 0,
                "auth": {"allow-anonymous": True, "plugins": ["auth_anonymous"]}, "topic-check": {"enabled": False}})
    await b.start()
    return b


async def stop_broker(b) -> None:
    try:
        await asyncio.wait_for(b.shutdown(), 5)
    except asyncio.TimeoutError:
        pass        # amqtt 0.11.0b1's shutdown sometimes never returns (seen 1 in ~2 runs after the bridge test)


@pytest.fixture
async def broker():
    port = free_port()
    b = await start_broker(port)
    yield f"mqtt://127.0.0.1:{port}", port
    await stop_broker(b)


async def publish(port: int, messages) -> None:
    async with aiomqtt.Client("127.0.0.1", port, identifier="transmission-test") as pub:
        for topic, payload in messages:
            await pub.publish(topic, payload, qos=0)


async def wait_for(condition, timeout: float = 10.0) -> None:
    async def poll():
        while not condition():
            await asyncio.sleep(0.02)
    await asyncio.wait_for(poll(), timeout)


# ----------------------------------------------------------------------------- client, no broker

def test_factory_makes_a_sparkplug_client():
    c = create_protocol_client("sparkplug", "chariot", "mqtt://chariot:1883", {"client_id": "eval"}, on_record=lambda r: None)
    assert isinstance(c, SparkplugClient)
    assert c.protocol_type.value == "sparkplug"
    assert (c.topics, c.qos, c.client_id) == (["spBv1.0/#"], 0, "eval")
    assert (c.host, c.port, c.use_tls) == ("chariot", 1883, False)


@pytest.mark.parametrize("endpoint, expected", [
    ("mqtt://chariot:1883", ("chariot", 1883, False)),
    ("mqtts://chariot:8883", ("chariot", 8883, True)),
    ("mqtts://chariot", ("chariot", 8883, True)),
    ("chariot", ("chariot", 1883, False)),
    ("10.0.0.5:1884", ("10.0.0.5", 1884, False)),
])
def test_the_endpoint_gives_host_port_and_tls(endpoint, expected):
    assert parse_endpoint(endpoint) == expected
    c = create_protocol_client("sparkplug", "s", endpoint, {}, on_record=lambda r: None)
    assert c.security_config.use_tls is expected[2]


def test_the_tls_settings_are_logged_at_connect(caplog):
    c = create_protocol_client("sparkplug", "s", "mqtts://chariot:8883",
                               {"username": "u", "security": {"ca_cert_path": "/certs/ca.pem"}}, on_record=lambda r: None)
    with caplog.at_level("INFO"):
        c.security_manager.log_security_status()
    assert "CA Certificate: /certs/ca.pem" in caplog.text and "Mutual TLS (mTLS): DISABLED" in caplog.text
    plain = create_protocol_client("sparkplug", "s", "mqtt://chariot:1883", {"username": "u"}, on_record=lambda r: None)
    with caplog.at_level("INFO"):
        plain.security_manager.log_security_status()
    assert "MQTT Security: DISABLED" in caplog.text


def test_factory_refuses_other_protocols():
    for protocol in ("mqtt", "opcua", "modbus"):
        with pytest.raises(ValueError, match="Unsupported protocol"):
            create_protocol_client(protocol, "s", "mqtt://chariot:1883", {}, on_record=lambda r: None)


def test_handle_message_hands_on_rows_and_counts_skips():
    rows: list[RawRow] = []
    c = SparkplugClient("chariot", "mqtt://chariot:1883", KEEP_ALL, on_record=rows.append)
    messages = transmission_messages()
    for topic, payload, _ in messages:
        assert c.handle_message(topic, payload, 1).row is not None
    for topic, payload, _ in SKIPPED:
        assert c.handle_message(topic, payload, 1).row is None
    stats = c.stats()
    assert stats["messages_received"] == len(messages) + len(SKIPPED) and stats["rows"] == len(messages) == len(rows)
    assert {k: v for k, v in stats.items() if k.startswith("skipped_") and v} == dict(Counter(s for _, _, s in SKIPPED))


def test_the_client_reports_its_stats_every_100_messages():
    reported = []
    c = SparkplugClient("chariot", "mqtt://chariot:1883", {}, on_record=lambda r: None, on_stats=reported.append)
    topic, payload, _ = transmission_messages()[0]
    for _ in range(250):
        c.handle_message(topic, payload, 1)
    assert [r["messages_received"] for r in reported] == [100, 200]


def test_a_row_the_bridge_cant_take_is_counted_and_the_next_messages_go_on():
    """An exception from on_record left the message loop: the subscription ended (a primary host OFFLINE)."""
    taken = []

    def on_record(row):
        if not taken:
            taken.append(None)
            raise RuntimeError("queue unavailable")
        taken.append(row)
    c = SparkplugClient("chariot", "mqtt://chariot:1883", KEEP_ALL, on_record=on_record)
    topic, payload, _ = transmission_messages()[0]
    c.handle_message(topic, payload, 1)
    c.handle_message(topic, payload, 2)
    assert len(taken) == 2 and taken[1] is not None
    assert c.stats()["rows"] == 1 and c.stats()["rows_failed"] == 1

async def test_a_host_taken_offline_while_subscribing_publishes_no_online_after_its_offline():
    """The supervisor took the host OFFLINE while subscribe() waited on a topic subscribe: an ONLINE published
    then came after the OFFLINE, retained, and the clean disconnect fires no will (the edges wouldn't buffer)."""
    published, release = [], asyncio.Event()

    class Mqtt:
        async def subscribe(self, topic, qos=0):
            await release.wait()

        async def publish(self, topic, payload, qos=0, retain=False):
            published.append(json.loads(payload)["online"])

        async def __aexit__(self, *exc_info):
            await asyncio.sleep(0.05)                       # a clean disconnect takes a moment

    c = SparkplugClient("chariot", "mqtt://chariot:1883", {"primary_host_id": "databricks-zerobus"}, on_record=lambda r: None)
    c._client, c.session_timestamp = Mqtt(), 1
    c.online_allowed.set()
    subscribing = asyncio.create_task(c.subscribe())
    await asyncio.sleep(0)                                  # waiting on its topic subscribe
    c.online_allowed.clear()                                # as the supervisor does before go_offline()
    offline = asyncio.create_task(c.go_offline())
    await asyncio.sleep(0.01)                               # OFFLINE published, disconnecting
    release.set()
    await asyncio.gather(subscribing, offline, return_exceptions=True)
    await c.close()
    assert published == [False]

# ----------------------------------------------------------------------------- through a broker

async def test_rows_from_a_broker_equal_java_tahu(broker):
    endpoint, port = broker
    rows: list[RawRow] = []
    client = SparkplugClient("chariot", endpoint, {"client_id": "sparkplug-test", **KEEP_ALL}, on_record=rows.append)
    await client.connect()
    task = asyncio.create_task(client.subscribe())
    try:
        await asyncio.wait_for(client.subscribed.wait(), 5)
        messages = transmission_messages()
        await publish(port, [(t, p) for t, p, _ in messages] + [(t, p) for t, p, _ in SKIPPED])
        subscribed_skips = [s for s in SKIPPED if s[0].startswith("spBv1.0/")]   # spBv1.0/# only: no legacy STATE, no foreign topics
        await wait_for(lambda: client.counters["messages_received"] == len(messages) + len(subscribed_skips))
    finally:
        await client.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await client.close()
    assert sorted((r.msg_topic, r.msg) for r in rows) == sorted((t, j) for t, _, j in messages)
    assert {(r.group_id, r.edge_node_id) for r in rows} == {("Plant1", "Line A")}
    assert client.counters["skipped_state"] == 1 and client.counters["skipped_command"] == 2
    assert client.counters["skipped_malformed_topic"] == 1 and client.counters["skipped_undecodable_payload"] == 1
    assert client.counters["skipped_foreign_topic"] == 0


async def test_bridge_queues_sparkplug_rows_with_the_sparkplug_raw_columns(broker):
    endpoint, _port = broker
    bridge = Bridge({
        "sources": [{"name": "chariot", "protocol": "sparkplug", "endpoint": endpoint, "client_id": "bridge-test", **KEEP_ALL}],
        "zerobus": {"enabled": False},
        "backpressure": {},
    })
    await bridge.start()
    try:
        await wait_for(lambda: "chariot" in bridge.clients)
        await asyncio.wait_for(bridge.clients["chariot"].subscribed.wait(), 5)
        messages = transmission_messages()
        await publish(_port, [(t, p) for t, p, _ in messages] + [(t, p) for t, p, _ in SKIPPED])
        await wait_for(lambda: bridge.backpressure.memory_queue.qsize() == len(messages))
        assert bridge.get_metrics()["clients"]["chariot"]["sparkplug"]["rows"] == len(messages)
    finally:
        await bridge.stop()
    assert all(c._client is None for c in bridge.clients.values())          # disconnected
    queued = [bridge.backpressure.memory_queue.get_nowait() for _ in range(len(messages))]
    written = [bridge._to_zerobus_message(q) for q in queued]
    assert all(tuple(w) == COLUMNS for w in written)
    assert sorted((w["msg_topic"], w["msg"]) for w in written) == sorted((t, j) for t, _, j in messages)
    assert all(w["inserted_at"].isdigit() and w["namespace"] == "spBv1.0" for w in written)
    assert bridge.metrics["records_received"] == len(messages)


def test_bridge_writes_a_row_unchanged():
    bridge = Bridge({"sources": [], "zerobus": {"enabled": False},
                            "backpressure": {}})
    row = convert("spBv1.0/Plant1/NDEATH/Line A", (TRANSMISSION / "ndeath-1.bin").read_bytes(), 1790694245123).row
    bridge._on_record(row)
    assert bridge._to_zerobus_message(bridge.backpressure.memory_queue.get_nowait()) == row.to_zerobus_record()


async def test_source_reconnects_and_receives_again_after_the_connection_drops(broker):
    """Like a Chariot restart from the client's side: the TCP connection goes away, the bridge's reconnect
    loop calls client.stop() and connects again, and rows flow again. (amqtt can't stand in for a broker
    restart: its shutdown leaves the old sessions answering.)"""
    endpoint, port = broker
    bridge = Bridge({
        "sources": [{"name": "chariot", "protocol": "sparkplug", "endpoint": endpoint, "client_id": "drop-test", **KEEP_ALL}],
        "zerobus": {"enabled": False},
        "backpressure": {},
    })
    topic, payload, _ = transmission_messages()[0]
    await bridge.start()
    try:
        await wait_for(lambda: "chariot" in bridge.clients)
        client = bridge.clients["chariot"]
        await asyncio.wait_for(client.subscribed.wait(), 5)
        await publish(port, [(topic, payload)])
        await wait_for(lambda: client.counters["rows"] == 1)
        client._client._client.socket().shutdown(socket.SHUT_RDWR)       # paho's socket: the connection drops
        await wait_for(lambda: bridge.metrics["reconnections"] >= 1, 20)
        await wait_for(lambda: client.subscribed.is_set(), 30)
        await publish(port, [(topic, payload)])
        await wait_for(lambda: client.counters["rows"] == 2, 20)
    finally:
        await bridge.stop()


async def test_stop_leaves_the_client_able_to_subscribe_again(broker):
    """The bridge calls stop() before reconnecting; a stop flag left set ended the next subscription."""
    endpoint, port = broker
    rows = []
    client = SparkplugClient("chariot", endpoint, {"client_id": "stop-test", **KEEP_ALL}, on_record=rows.append)
    topic, payload, _ = transmission_messages()[0]
    for attempt in (1, 2):
        await client.connect()
        task = asyncio.create_task(client.subscribe())
        await asyncio.wait_for(client.subscribed.wait(), 5)
        await publish(port, [(topic, payload)])
        await wait_for(lambda: len(rows) == attempt)
        await client.stop()
        await asyncio.gather(task, return_exceptions=True)
    await client.close()


async def test_a_disconnect_that_hangs_is_given_up_so_the_source_can_connect_again(broker, monkeypatch):
    """On a half-open connection aiomqtt's exit can wait for good, and connect() does nothing while a client is
    set: disconnect() drops the client after DISCONNECT_TIMEOUT_S."""
    monkeypatch.setattr(sparkplug_client, "DISCONNECT_TIMEOUT_S", 0.05)
    endpoint, port = broker
    rows = []
    client = SparkplugClient("chariot", endpoint, {"client_id": "hang-test", **KEEP_ALL}, on_record=rows.append)
    await client.connect()
    stuck = client._client

    async def hang(*exc_info):
        await asyncio.Event().wait()
    stuck.__aexit__ = hang
    try:
        await asyncio.wait_for(client.disconnect(), 2)
        assert not client.is_connected and not client.online
        await client.connect()
        assert client.is_connected and client._client is not stuck
        task = asyncio.create_task(client.subscribe())
        await asyncio.wait_for(client.subscribed.wait(), 5)
        topic, payload, _ = transmission_messages()[0]
        await publish(port, [(topic, payload)])
        await wait_for(lambda: len(rows) == 1)
        await client.stop()
        await asyncio.gather(task, return_exceptions=True)
        await client.close()
    finally:
        await type(stuck).__aexit__(stuck, None, None, None)
