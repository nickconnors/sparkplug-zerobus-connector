"""Numeric compression in the bridge, with the per-target queues, the write-ahead log and the web page:
per-source settings follow source changes, NaN readings, Sparkplug rows never compressed."""
from __future__ import annotations

import asyncio
import json
import math

from conftest import FIXTURES
from test_confirmed_writer import FakeStreams
from test_web_sources import page, started  # noqa: F401  (fixtures)
from unified_connector.core import unified_bridge
from unified_connector.protocols.base import ProtocolRecord, ProtocolType
from unified_connector.sparkplug.raw_row import convert

T = 1790694245000
NDEATH = (FIXTURES / "transmission" / "ndeath-1.bin").read_bytes()
TARGET = {"workspace_host": "https://example.cloud.databricks.com", "catalog": "c", "schema": "s", "table": "t"}


def config(sources=(), compression=None, **zerobus) -> dict:
    return {
        "sources": [dict(s) for s in sources],
        "zerobus": {"enabled": False, "default_target": TARGET, **zerobus},
        "normalization": {"enabled": False},
        "backpressure": {"disk_spool": {"enabled": False}},
        "numeric_compression": compression or {},
    }


def reading(value, i: int, source: str = "o", path: str = "Objects/Line1/Speed") -> ProtocolRecord:
    num = float(value) if isinstance(value, (int, float)) else None
    return ProtocolRecord(T + 1000 * i, source, "opc.tcp://plc:4840", ProtocolType.OPCUA, path, value, "float", num)


def queued(bridge, source: str = "o") -> list:
    q = bridge._queue_for_source(source).memory_queue
    return [q.get_nowait()["value"] for _ in range(q.qsize())]


def feed(bridge, values, source: str = "o") -> list:
    for i, v in enumerate(values):
        bridge._on_record(reading(v, i, source), source)
    return queued(bridge, source)


def same(a: list, b: list) -> bool:
    return len(a) == len(b) and all(x == y or (math.isnan(x) and math.isnan(y)) for x, y in zip(a, b))


OPCUA = {"name": "o", "protocol": "opcua", "endpoint": "opc.tcp://plc:4840"}


def test_a_nan_first_reading_doesnt_stop_a_deadband_tag():
    bridge = unified_bridge.UnifiedBridge(config(compression={"mode": "deadband", "deadband": 0.5}))
    assert same(feed(bridge, [math.nan, 1.0, 1.2, 5.0, math.nan, math.nan, 6.0]), [math.nan, 1.0, 5.0, math.nan, 6.0])


def test_a_nan_reading_doesnt_stop_an_sdt_tag():
    """Before: a NaN made every SDT slope NaN, so nothing more was emitted until sdt_max_interval_ms."""
    bridge = unified_bridge.UnifiedBridge(config(compression={"mode": "sdt", "sdt_deviation": 0.25,
                                                              "sdt_max_interval_ms": 600000}))
    assert same(feed(bridge, [math.nan, 1.0, 5.0, 50.0, 0.0]), [math.nan, 1.0, 5.0, 50.0])
    bridge = unified_bridge.UnifiedBridge(config(compression={"mode": "sdt", "sdt_deviation": 0.25}))
    assert same(feed(bridge, [1.0, 1.0, math.nan, math.nan, 2.0]), [1.0, math.nan, 2.0])


def test_change_only_passes_a_repeated_nan_once():
    bridge = unified_bridge.UnifiedBridge(config(compression={"mode": "change_only"}))
    assert same(feed(bridge, [math.nan, math.nan, 1.0, math.inf, math.inf, 1.0]), [math.nan, 1.0, math.inf, 1.0])


async def test_an_edited_source_uses_its_new_compression_settings():
    bridge = unified_bridge.UnifiedBridge(config([{**OPCUA, "numeric_compression": {"mode": "deadband", "deadband": 10}}]))
    assert feed(bridge, [1.0, 2.0, 3.0]) == [1.0]
    await bridge.update_source("o", {**OPCUA, "numeric_compression": {"mode": "change_only"}})
    assert feed(bridge, [3.0, 3.0, 4.0]) == [3.0, 4.0]   # 3.0 again: the edit started its compression again
    await bridge.update_source("o", dict(OPCUA))           # no override: the global policy (none)
    assert feed(bridge, [4.0, 4.0]) == [4.0, 4.0]


async def test_a_removed_source_leaves_no_compression_state():
    bridge = unified_bridge.UnifiedBridge(config([OPCUA, {**OPCUA, "name": "p"}], {"mode": "change_only"}))
    feed(bridge, [1.0], "o")
    feed(bridge, [1.0], "p")
    await bridge.remove_source("o")
    assert list(bridge.last_emitted_by_key) == ["p::Objects/Line1/Speed"]
    assert bridge._source_compression.keys() == {"p"}


async def test_saving_the_zerobus_settings_keeps_the_sources_compression_settings(page):   # noqa: F811
    http, path, bridge = page
    text = path.read_text().replace("- name: m\n", "- name: m\n  numeric_compression:\n    mode: change_only\n")
    path.write_text(text)
    bridge._source_compression.clear()
    async with http.post("/api/zerobus/config", json={"batch": {"max_records": 250}}) as r:
        assert r.status == 200, await r.text()
    policy, _ = bridge._source_compression["m"]
    assert policy is not None and policy.mode.value == "change_only"


def test_sparkplug_rows_are_never_compressed():
    bridge = unified_bridge.UnifiedBridge(config([{"name": "sp", "protocol": "sparkplug", "endpoint": "mqtt://b:1883",
                                                   "numeric_compression": {"mode": "change_only"}}],
                                                 {"mode": "change_only"}))
    row = convert("spBv1.0/G/NDEATH/A", NDEATH, T).row
    for _ in range(3):
        bridge._on_record(row, "sp")
    assert bridge._queue_for_source("sp").memory_queue.qsize() == 3
    assert bridge.metrics["compression"]["incoming"] == 0


async def test_filtered_rows_never_enter_the_write_ahead_log(monkeypatch, tmp_path):
    streams = FakeStreams()

    class FakeRowStreams:
        def __init__(self, *args):
            pass

        async def open(self):
            return await streams.open()

    monkeypatch.setattr(unified_bridge, "ZerobusRowStreams", FakeRowStreams)
    bridge = unified_bridge.UnifiedBridge(config(
        [OPCUA], {"mode": "change_only"}, enabled=True, confirmed_writes=True, zerobus_endpoint="https://example.zerobus",
        auth={"client_id": "id", "client_secret": "secret"}, wal={"enabled": True, "path": str(tmp_path / "wal")},
        batch={"max_records": 500, "linger_seconds": 0.02}))
    await bridge.start_infra()
    try:
        for i, v in enumerate([1.0, 1.0, 1.0, 2.0, 2.0]):
            bridge._on_record(reading(v, i), "o")
        async with asyncio.timeout(5):
            while sum(map(len, streams.ingested)) < 2:
                await asyncio.sleep(0.01)
        status = next(iter(bridge.wals.values())).status()
    finally:
        await bridge.stop()
    assert [json.loads(r)["value"] for batch in streams.ingested for r in batch] == ["1.0", "2.0"]
    assert status.rows_appended == 2
    metrics = bridge.get_metrics()["bridge"]["compression"]
    assert (metrics["incoming"], metrics["emitted"], metrics["filtered_change_only"]) == (5, 2, 3)
