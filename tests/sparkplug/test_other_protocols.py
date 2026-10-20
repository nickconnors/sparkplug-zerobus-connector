"""OPC-UA, MQTT and Modbus records through the bridge's per-target queues: each to its source's table, in the
UC OPC-UA bronze schema, on the original write path and with confirmed writes and the write-ahead log (a crash
in between included)."""
from __future__ import annotations

import asyncio
import datetime
import inspect
import json
import time

import pytest

from test_confirmed_writer import FakeStreams
from test_per_target_queues import config, wait_for
from test_source_targets import original_path  # noqa: F401 - a fixture
from test_sparkplug_source import broker, free_port  # noqa: F401 - broker: a fixture
from test_wal import crash
from unified_connector.core import unified_bridge
from unified_connector.protocols.base import ProtocolRecord, ProtocolType
from unified_connector.protocols.factory import create_protocol_client

T = 1790694245123


def records(source: str) -> list[ProtocolRecord]:
    """Records as opcua_client, mqtt_client and modbus_client make them (an OPC-UA DateTime value: no JSON type)."""
    return [
        ProtocolRecord(T, source, "opc.tcp://plc:4840", ProtocolType.OPCUA, "Objects/Line1/Speed", 12.5, "float", 12.5,
                       metadata={"namespace": 2, "node_id": "NodeId(Identifier=1001, NamespaceIndex=2)", "status_code": 0}),
        ProtocolRecord(T + 1, source, "opc.tcp://plc:4840", ProtocolType.OPCUA, "Objects/Line1/LastStart",
                       datetime.datetime(2026, 10, 20, 12, 0, tzinfo=datetime.timezone.utc), "datetime",
                       metadata={"node_id": "NodeId(Identifier=1002, NamespaceIndex=2)"},
                       status_code=0x40000000, status="Uncertain"),
        ProtocolRecord(T + 2, source, "mqtt://broker:1883", ProtocolType.MQTT, "plant/line1/temperature", 21.5, "float",
                       21.5, metadata={"qos": 0}),
        ProtocolRecord(T + 3, source, "modbus://plc:502", ProtocolType.MODBUS, "holding/40001", 7, "int", 7.0,
                       metadata={"register_type": "holding", "address": 40001, "unit_id": 1, "raw_value": 7,
                                 "scale": 1, "offset": 0}),
    ]


def bronze(source: str) -> list[dict]:
    """The rows Zerobus gets for records(source), ingest_time aside."""
    common = {"source_name": source, "status_code": 0, "status": "Good", "raw": "", "plc_name": "", "plc_vendor": "",
              "plc_model": ""}
    opcua = {**common, "endpoint": "opc.tcp://plc:4840", "namespace": 2, "vendor_format": "opcua"}
    return [
        {**opcua, "event_time": T * 1000, "node_id": "ns=2;i=1001", "browse_path": "Objects/Line1/Speed",
         "value_type": "float", "value": "12.5", "value_num": 12.5},
        {**opcua, "event_time": (T + 1) * 1000, "node_id": "ns=2;i=1002", "browse_path": "Objects/Line1/LastStart",
         "status_code": 0x40000000, "status": "Uncertain", "value_type": "datetime", "value": "2026-10-20 12:00:00+00:00"},
        {**common, "event_time": (T + 2) * 1000, "endpoint": "mqtt://broker:1883", "namespace": 0, "node_id": "",
         "browse_path": "plant/line1/temperature", "value_type": "float", "value": "21.5", "value_num": 21.5,
         "vendor_format": "generic"},
        {**common, "event_time": (T + 3) * 1000, "endpoint": "modbus://plc:502", "namespace": 0, "node_id": "",
         "browse_path": "holding/40001", "value_type": "int", "value": "7", "value_num": 7.0, "vendor_format": "modbus"},
    ]


def send(bridge) -> None:
    """Source b's records (source_targets: table_b) by name, and one of a's with only its own source_name."""
    for r in records("b"):
        bridge._on_record(r, "b")
    bridge._on_record(records("a")[2])


EXPECTED = {"c.s.table_a": [bronze("a")[2]], "c.s.table_b": bronze("b")}


def without_ingest_time(row: dict) -> dict:
    assert isinstance(row.pop("ingest_time"), int)
    return row


class TableStreams:
    """A FakeStreams per table (ZerobusRowStreams is given the table's name)."""

    def __init__(self):
        self.tables: dict[str, FakeStreams] = {}
        self.unreachable = False

    def written(self) -> dict[str, list[dict]]:
        return {t: [without_ingest_time(json.loads(j)) for b in s.ingested for j in b] for t, s in self.tables.items()}


@pytest.fixture
def streams(monkeypatch):
    made = TableStreams()

    class FakeRowStreams:
        def __init__(self, endpoint, workspace_url, table, *args):
            self.streams = made.tables.setdefault(table, FakeStreams())

        async def open(self):
            if made.unreachable:
                raise ConnectionError("workspace unreachable")
            return await self.streams.open()

    monkeypatch.setattr(unified_bridge, "ZerobusRowStreams", FakeRowStreams)
    return made


async def test_the_original_path_hands_records_to_the_sdk_without_an_acknowledgement_left_unawaited():
    from zerobus.sdk.aio import ZerobusStream

    from unified_connector.core.zerobus_client import ZeroBusClient

    assert list(inspect.signature(ZerobusStream.ingest_record_offset).parameters)[1:] == ["payload"]
    sent = []

    class Stream:
        async def ingest_record_offset(self, payload):
            sent.append(payload)
            return len(sent) - 1

        async def ingest_record(self, payload):
            raise AssertionError("ingest_record: SDK 1.x returns a coroutine for the acknowledgement, never awaited")

    client = ZeroBusClient({"zerobus": {"workspace_host": "https://example.cloud.databricks.com",
                                        "zerobus_endpoint": "https://example.zerobus",
                                        "auth": {"client_id": "id", "client_secret": "secret"},
                                        "target": {"catalog": "c", "schema": "s", "table": "t"},
                                        "retry": {"max_attempts": 1}}})
    client._stream = Stream()
    batch = [{"n": 1}, {"n": 2}]
    assert await client.send_batch(batch) is True
    assert sent == batch and client.metrics["records_sent"] == 2


async def test_on_the_original_path_each_table_gets_its_sources_rows_in_the_bronze_schema(tmp_path, original_path):
    bridge = unified_bridge.UnifiedBridge(config(tmp_path, confirmed=False))
    await bridge.start_infra()
    try:
        assert bridge.writers == {}
        send(bridge)
        await wait_for(lambda: sum(len(v) for v in original_path.sent.values()) == 5)
    finally:
        await bridge.stop()
    assert {t: [without_ingest_time(m) for m in v] for t, v in original_path.sent.items()} == EXPECTED


@pytest.mark.parametrize("crash_first", [False, True])
async def test_with_confirmed_writes_and_the_log_each_table_gets_its_sources_rows_in_the_bronze_schema(
        tmp_path, streams, crash_first):
    cfg = config(tmp_path, confirmed=True)
    cfg["zerobus"]["wal"] = {"enabled": True, "path": str(tmp_path / "wal"), "fsync_interval_seconds": 0.05}
    if crash_first:                                       # logged, never written: replayed from the log
        streams.unreachable = True
        first = unified_bridge.UnifiedBridge(cfg)
        await first.start_infra()
        send(first)
        wal = first.get_metrics()["wal"]
        assert (wal["example.c.s.table_a"]["pending"], wal["example.c.s.table_b"]["pending"]) == (1, 4)
        await crash(first)
        streams.unreachable = False
    bridge = unified_bridge.UnifiedBridge(cfg)
    await bridge.start_infra()
    if not crash_first:
        send(bridge)
    await bridge.stop()                                   # drains: everything queued is written first
    assert streams.written() == EXPECTED
    wal = bridge.get_metrics()["wal"]
    assert {t: (s["pending"], s["rows_replayed"]) for t, s in wal.items()} == {
        "example.c.s.table_a": (0, 1 if crash_first else 0), "example.c.s.table_b": (0, 4 if crash_first else 0)}


async def test_a_value_that_isnt_a_finite_number_has_no_value_num(tmp_path, streams):
    """A NaN or infinite reading (an OPC-UA Double from a failed sensor, an MQTT payload NaN) is written with its
    value as text and no value_num: JSON has no NaN, and the row isn't lost."""
    bridge = unified_bridge.UnifiedBridge(config(tmp_path, confirmed=True))
    await bridge.start_infra()
    readings = [float("nan"), float("inf"), float("-inf")]
    for i, v in enumerate(readings):
        bridge._on_record(ProtocolRecord(T + i, "a", "opc.tcp://plc:4840", ProtocolType.OPCUA, "Objects/Line1/Speed", v,
                                         "float", v), "a")
    await bridge.stop()
    rows = [json.loads(j, parse_constant=lambda c: pytest.fail(f"{c} written")) for b in streams.tables["c.s.table_a"].ingested
            for j in b]
    assert [(r["value"], "value_num" in r) for r in rows] == [("nan", False), ("inf", False), ("-inf", False)]

# ----------------------------------------------------------------------------- stopping and reconnecting sources

@pytest.mark.parametrize("protocol, endpoint", [("mqtt", "mqtt://127.0.0.1:1883"), ("modbus", "modbus://127.0.0.1:502"),
                                                ("opcua", "opc.tcp://127.0.0.1:4840")])
async def test_every_source_can_be_stopped(protocol, endpoint):
    """The bridge calls stop() and then disconnect() on every client it stops (and after a connection error)."""
    client = create_protocol_client(protocol, "s", endpoint, {}, on_record=lambda r: None)
    await client.stop()
    await client.disconnect()


async def test_stopping_an_mqtt_source_disconnects_it(broker, caplog):
    endpoint, _ = broker
    bridge = unified_bridge.UnifiedBridge({
        "sources": [{"name": "m", "protocol": "mqtt", "endpoint": endpoint, "topics": ["plant/#"], "client_id": "stop-mqtt"}],
        "zerobus": {"enabled": False}, "normalization": {"enabled": False},
        "backpressure": {"disk_spool": {"enabled": False, "encryption": False}},
    })
    await bridge.start()
    try:
        await wait_for(lambda: "m" in bridge.clients and bridge.clients["m"]._client is not None)
        client = bridge.clients["m"]
        assert (await bridge.stop_source("m"))["status"] == "ok"
        assert client._client is None and "m" not in bridge.clients
        assert (await bridge.start_source("m"))["status"] == "ok"
        await wait_for(lambda: bridge.clients["m"]._client is not None)
    finally:
        started = time.monotonic()
        await bridge.stop()
    assert time.monotonic() - started < 4                 # its task ends on the disconnect, not the 5 s timeout
    assert "Error stopping" not in caplog.text


async def opcua_server(port: int):
    from asyncua import Server

    server = Server()
    await server.init()
    server.set_endpoint(f"opc.tcp://127.0.0.1:{port}/test/")
    ns = await server.register_namespace("urn:test")
    line = await server.nodes.objects.add_object(ns, "Line1")
    await line.add_variable(ns, "Speed", 12.5)
    await server.start()
    return server


async def test_an_opcua_source_polls_again_after_the_bridge_reconnects_it():
    """After a connection error the bridge calls stop() and disconnect() and connects the same client again
    (_client_lifecycle): the stop must not end the new connection's polling at once."""
    port = free_port()
    server = await opcua_server(port)
    got = []
    client = create_protocol_client("opcua", "plc", f"opc.tcp://127.0.0.1:{port}/test/",
                                    {"polling_interval_ms": 50, "variable_limit": 20}, on_record=got.append)
    try:
        for _ in range(2):
            await client.connect()
            task = asyncio.create_task(client.subscribe())
            before = len(got)
            await wait_for(lambda: len(got) >= before + 3, 15)
            await client.stop()
            await client.disconnect()
            await asyncio.wait_for(task, 5)
    finally:
        await client.disconnect()
        await server.stop()
