"""A source's own target table (target_table): each table its own queue and confirmed writer."""
from __future__ import annotations

import json

import pytest

from test_confirmed_writer import FakeStreams
from test_per_target_queues import rows, wait_for
from test_wal import bridge_config, fake_streams  # noqa: F401 - a fixture
from test_web_sources import page, saved, started  # noqa: F401 - fixtures
from test_zerobus_restart import hosts  # noqa: F401 - a fixture
from sparkplug_connector.core import bridge as core_bridge, targets

DEFAULT = "example.c.s.sparkplug_raw"


def sparkplug(name="sp", **extra):
    return {"name": name, "protocol": "sparkplug", "endpoint": "mqtt://localhost:1883", **extra}


# ----------------------------------------------------------------------------- the rules

def test_a_target_table_is_catalog_schema_table():
    assert targets.parse_target_table(" my-cat.stage_db.sparkplug_raw ") == ("my-cat", "stage_db", "sparkplug_raw")
    for bad in ("sparkplug_raw", "s.t", "a.b.c.d", "a..t", "a.b.t x", "a.b.`t`"):
        with pytest.raises(ValueError, match="catalog.schema.table"):
            targets.parse_target_table(bad)


def test_the_source_s_own_table_wins_then_source_targets_then_the_default():
    zerobus = {"default_target": {"workspace_host": "w", "catalog": "c", "schema": "s", "table": "t"},
               "source_targets": {"a": {"table": "from_source_targets"}}}
    table = lambda source: targets.table_name(targets.target_config(source, zerobus))  # noqa: E731
    assert table({"name": "a", "target_table": "x.y.own"}) == "x.y.own"
    assert table({"name": "a"}) == "c.s.from_source_targets"
    assert table({"name": "b"}) == "c.s.t"
    assert targets.target_config({"name": "a", "target_table": "x.y.own"}, zerobus)["workspace_host"] == "w"


# ----------------------------------------------------------------------------- the bridge

async def test_each_source_writes_its_own_table(tmp_path, monkeypatch, hosts):
    streams: dict[str, FakeStreams] = {}

    class FakeRowStreams:
        def __init__(self, endpoint, workspace_url, table, *args):
            self.table = table
            streams.setdefault(table, FakeStreams())

        async def open(self):
            return await streams[self.table].open()

    monkeypatch.setattr(core_bridge, "ZerobusRowStreams", FakeRowStreams)
    cfg = bridge_config(tmp_path)
    cfg["sources"] = [sparkplug(target_table="c.s.sparkplug_raw_2"), sparkplug("other")]
    bridge = core_bridge.Bridge(cfg)
    await bridge.start()
    try:
        assert sorted(bridge.writers) == [DEFAULT, "example.c.s.sparkplug_raw_2"]
        own, default = rows("A", 2), rows("B", 3)
        for r in own:
            bridge._on_record(r, "sp")
        for r in default:
            bridge._on_record(r, "other")
        await wait_for(lambda: sum(len(b) for s in streams.values() for b in s.ingested) == 5)
    finally:
        await bridge.stop()
    written = {table: [json.loads(j)["msg_id"] for b in s.ingested for j in b] for table, s in streams.items()}
    assert written == {"c.s.sparkplug_raw_2": [r.msg_id for r in own], "c.s.sparkplug_raw": [r.msg_id for r in default]}


async def test_a_source_with_a_malformed_target_table_isnt_started(tmp_path, fake_streams, hosts):
    cfg = bridge_config(tmp_path)
    cfg["sources"] = [sparkplug(target_table="bronze")]
    bridge = core_bridge.Bridge(cfg)
    await bridge.start()
    try:
        assert bridge.clients == {}
        result = await bridge.start_source("sp")
        assert result["status"] == "error" and "catalog.schema.table" in result["message"]
    finally:
        await bridge.stop()


async def test_a_source_s_new_table_is_set_up_when_it_starts(tmp_path, fake_streams, hosts):
    bridge = core_bridge.Bridge(bridge_config(tmp_path))
    await bridge.start()
    try:
        await bridge.add_source("sp2", "sparkplug", {"endpoint": "mqtt://localhost:1883", "target_table": "c.s.new_table"})
        assert "example.c.s.new_table" in bridge.writers and "example.c.s.new_table" in bridge.writer_tasks
        sent = rows("A", 2)
        for r in sent:
            bridge._on_record(r, "sp2")
        await wait_for(lambda: sum(len(b) for b in fake_streams.ingested) == 2)
    finally:
        await bridge.stop()


# ----------------------------------------------------------------------------- the web page

async def test_the_page_refuses_a_malformed_target_table(page, started):
    http, path, bridge = page
    before = path.read_text()
    async with http.post("/api/sources", json={"name": "sp3", "protocol": "sparkplug", "endpoint": "mqtt://localhost:1883",
                                               "enabled": True, "target_table": "bronze"}) as r:
        assert r.status == 400 and "catalog.schema.table" in (await r.json())["message"]
    async with http.put("/api/sources/m", json={"name": "m", "protocol": "sparkplug", "endpoint": "mqtt://localhost:1883",
                                                "enabled": True, "target_table": "a.b"}) as r:
        assert r.status == 400 and "catalog.schema.table" in (await r.json())["message"]
    assert path.read_text() == before


async def test_a_renamed_source_keeps_its_table(page, started):
    http, path, bridge = page
    table = bridge._get_target_for_source("sp")
    async with http.put("/api/sources/sp", json={"name": "sp-renamed", "protocol": "sparkplug",
                                                 "endpoint": "mqtt://localhost:1883", "enabled": True}) as r:
        assert r.status == 200, await r.text()
    assert "sp-renamed" in saved(path) and saved(path)["sp-renamed"]["target_table"] == "c.s.sparkplug_raw"
    assert bridge._get_target_for_source("sp-renamed") == table
