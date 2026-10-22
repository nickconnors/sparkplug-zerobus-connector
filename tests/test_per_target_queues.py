"""One backpressure queue per Zerobus target: each target's writer takes only its own sources' rows."""
from __future__ import annotations

import asyncio
import json

from conftest import FIXTURES
from test_confirmed_writer import FakeStreams
from sparkplug_connector.core import bridge as core_bridge
from sparkplug_connector.sparkplug.raw_row import convert

NDEATH = (FIXTURES / "transmission" / "ndeath-1.bin").read_bytes()


def config(tmp_path) -> dict:
    """Sources a (the default table, table_a) and b (source_targets: table_b), both enabled."""
    target = {"workspace_host": "https://example.cloud.databricks.com", "catalog": "c", "schema": "s"}
    return {
        "sources": [{"name": n, "protocol": "sparkplug", "endpoint": "mqtt://127.0.0.1:1", "enabled": True}
                    for n in ("a", "b")],
        "zerobus": {"enabled": True, "zerobus_endpoint": "https://example.zerobus",
                    "auth": {"client_id": "id", "client_secret": "secret"},
                    "default_target": {**target, "table": "table_a"},
                    "source_targets": {"b": {"table": "table_b"}},
                    "batch": {"max_records": 500, "timeout_seconds": 0.05, "linger_seconds": 0.02}},
        "backpressure": {},
    }


def rows(edge: str, n: int):
    return [convert(f"spBv1.0/G/NDEATH/{edge}", NDEATH, 1790694245123 + i).row for i in range(n)]


async def wait_for(condition, timeout=5.0):
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.01)


async def test_each_target_gets_only_its_sources_rows(monkeypatch, tmp_path):
    streams: dict[str, FakeStreams] = {}

    class FakeRowStreams:
        def __init__(self, endpoint, workspace_url, table, *args):
            self.table = table
            streams.setdefault(table, FakeStreams())

        async def open(self):
            return await streams[self.table].open()

    monkeypatch.setattr(core_bridge, "ZerobusRowStreams", FakeRowStreams)
    bridge = core_bridge.Bridge(config(tmp_path))
    await bridge.start_infra()
    try:
        a, b = rows("A", 5), rows("B", 3)
        for x, y in zip(a, b):
            bridge._on_record(x, "a")
            bridge._on_record(y, "b")
        for x in a[len(b):]:
            bridge._on_record(x, "a")
    finally:
        await bridge.stop()
    written = {table: [json.loads(j)["msg_id"] for batch in s.ingested for j in batch] for table, s in streams.items()}
    assert written == {"c.s.table_a": [r.msg_id for r in a], "c.s.table_b": [r.msg_id for r in b]}
    assert sorted(bridge.queues) == ["example.c.s.table_a", "example.c.s.table_b"]
    metrics = bridge.get_metrics()["backpressure"]
    assert metrics["records_enqueued"] == 8
    assert {t: m["records_enqueued"] for t, m in metrics["targets"].items()} == {"example.c.s.table_a": 5, "example.c.s.table_b": 3}


async def test_a_source_s_records_go_to_its_target_s_queue(monkeypatch, tmp_path):
    """The bridge binds each client's records to its source name (a RawRow carries none)."""
    handed = {}

    class IdleClient:
        primary_host_id = None

        def __init__(self, on_record):
            self.on_record = on_record

        async def connect(self):
            await asyncio.Event().wait()

    def factory(protocol_type, source_name, endpoint, config, on_record, on_stats):
        handed[source_name] = IdleClient(on_record)
        return handed[source_name]

    monkeypatch.setattr(core_bridge, "create_protocol_client", factory)
    bridge = core_bridge.Bridge(config(tmp_path))
    for name in ("a", "b"):
        await bridge._start_client({"name": name, "protocol": "sparkplug", "endpoint": "mqtt://localhost:1883"})
    try:
        handed["b"].on_record(rows("B", 1)[0])
        handed["a"].on_record(rows("A", 1)[0])
        handed["a"].on_record(rows("A", 1)[0])
        assert {t: q.memory_queue.qsize() for t, q in bridge.queues.items()} == {"example.c.s.table_a": 2, "example.c.s.table_b": 1}
        bridge._on_record(rows("X", 1)[0])                 # no source: the default target
        assert bridge.backpressure is bridge.queues["example.c.s.table_a"]
        assert bridge.backpressure.memory_queue.qsize() == 3
    finally:
        for task in bridge.client_tasks.values():
            task.cancel()
        await asyncio.gather(*bridge.client_tasks.values(), return_exceptions=True)
