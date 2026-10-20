"""A source's own target table (target_table), confirmed writes per table (always for a table that gets
Sparkplug rows), and the rule that Sparkplug rows and other rows don't share a table."""
from __future__ import annotations

import json

import pytest

from test_per_target_queues import rows, wait_for
from test_wal import bridge_config, fake_streams  # noqa: F401 - a fixture
from test_web_sources import page, saved, started  # noqa: F401 - fixtures
from test_zerobus_restart import hosts  # noqa: F401 - a fixture
from unified_connector.core import targets, unified_bridge

DEFAULT = "example.c.s.sparkplug_raw"


def mqtt(name="m", **extra):
    return {"name": name, "protocol": "mqtt", "endpoint": "mqtt://localhost:1883", **extra}


def sparkplug(name="sp", **extra):
    return {"name": name, "protocol": "sparkplug", "endpoint": "mqtt://localhost:1883", **extra}


class OriginalPath:
    """The original ZeroBusClient path (no network)."""
    sent: dict[str, list] = {}

    def __init__(self, cfg):
        t = cfg["zerobus"]["target"]
        self.full_table_name = f"{t['catalog']}.{t['schema']}.{t['table']}"
        self.zerobus_endpoint, self.workspace_url, self.client_id, self.client_secret = "e", "w", "id", "s"
        self.closed = False

    async def connect(self):
        pass

    async def send_batch(self, batch):
        OriginalPath.sent.setdefault(self.full_table_name, []).extend(batch)
        return True

    async def close(self):
        self.closed = True

    def get_metrics(self):
        return {}


@pytest.fixture
def original_path(monkeypatch):
    OriginalPath.sent = {}
    monkeypatch.setattr(unified_bridge, "ZeroBusClient", OriginalPath)
    return OriginalPath


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


def test_sparkplug_rows_and_other_rows_dont_share_a_table():
    by_table = lambda s: (s.get("target_table", "c.s.t"), s.get("target_table", "c.s.t"))  # noqa: E731
    assert targets.mixed_tables([sparkplug("a"), sparkplug("b"), mqtt("m", target_table="c.s.bronze")], by_table) == []
    assert targets.mixed_tables([mqtt("m"), {"name": "o", "protocol": "opcua"}], by_table) == []
    assert targets.mixed_tables([sparkplug("a"), mqtt("m"), sparkplug("b")], by_table) == [
        "table c.s.t would get Sparkplug rows (a, b) and other rows (m): give one of them a target table of its own"]


# ----------------------------------------------------------------------------- the bridge

async def test_only_the_sparkplug_table_has_confirmed_writes_and_each_source_writes_its_own(
        tmp_path, fake_streams, original_path, hosts):
    cfg = bridge_config(tmp_path)
    cfg["zerobus"]["confirmed_writes"] = False
    cfg["zerobus"]["batch"]["timeout_seconds"] = 0.05        # the original path's flush interval
    cfg["sources"] = [sparkplug(target_table="c.s.sparkplug_raw_2"), mqtt()]
    bridge = unified_bridge.UnifiedBridge(cfg)
    await bridge.start()
    try:
        assert list(bridge.writers) == ["example.c.s.sparkplug_raw_2"]   # the MQTT source's table: the original path
        sp_rows, m_rows = rows("A", 2), rows("B", 3)
        for r in sp_rows:
            bridge._on_record(r, "sp")
        for r in m_rows:
            bridge._on_record(r, "m")
        await wait_for(lambda: fake_streams.ingested and original_path.sent.get("c.s.sparkplug_raw"))
        await wait_for(lambda: len(original_path.sent["c.s.sparkplug_raw"]) == 3)
    finally:
        await bridge.stop()
    assert [json.loads(j)["msg_id"] for b in fake_streams.ingested for j in b] == [r.msg_id for r in sp_rows]
    assert [m["msg_id"] for m in original_path.sent["c.s.sparkplug_raw"]] == [r.msg_id for r in m_rows]


async def test_a_sparkplug_source_on_a_table_with_other_rows_isnt_started(tmp_path, fake_streams, hosts):
    cfg = bridge_config(tmp_path)
    cfg["sources"] = [sparkplug(), mqtt()]                    # both on the default table
    bridge = unified_bridge.UnifiedBridge(cfg)
    await bridge.start()
    try:
        assert sorted(bridge.clients) == ["m"]
        result = await bridge.start_source("sp")
        assert result["status"] == "error" and "also gets other rows (m)" in result["message"]
    finally:
        await bridge.stop()


async def test_a_source_s_new_table_is_set_up_when_it_starts(tmp_path, fake_streams, hosts):
    bridge = unified_bridge.UnifiedBridge(bridge_config(tmp_path))
    await bridge.start()
    try:
        await bridge.add_source("sp", "sparkplug", {"endpoint": "mqtt://localhost:1883", "target_table": "c.s.new_table"})
        assert "example.c.s.new_table" in bridge.writers and "example.c.s.new_table" in bridge.batch_tasks
        sent = rows("A", 2)
        for r in sent:
            bridge._on_record(r, "sp")
        await wait_for(lambda: sum(len(b) for b in fake_streams.ingested) == 2)
    finally:
        await bridge.stop()


async def test_a_table_on_the_original_path_gets_confirmed_writes_with_its_first_sparkplug_source(
        tmp_path, fake_streams, original_path, hosts):
    cfg = bridge_config(tmp_path)
    cfg["zerobus"]["confirmed_writes"] = False
    bridge = unified_bridge.UnifiedBridge(cfg)
    await bridge.start()                                      # no sources: the default table on the original path
    try:
        old = bridge.zerobus_clients[DEFAULT]
        assert DEFAULT not in bridge.writers
        await bridge.add_source("sp", "sparkplug", {"endpoint": "mqtt://localhost:1883"})
        assert DEFAULT in bridge.writers and old.closed and bridge.zerobus_clients[DEFAULT] is not old
        r = rows("A", 1)[0]
        bridge._on_record(r, "sp")
        await wait_for(lambda: fake_streams.ingested)
        assert original_path.sent == {}
    finally:
        await bridge.stop()


# ----------------------------------------------------------------------------- the web page

async def test_the_page_refuses_a_table_shared_by_sparkplug_and_other_rows(page, started):
    http, path, bridge = page                                 # sp on c.s.sparkplug_raw, m on the default table
    before = path.read_text()
    async with http.post("/api/sources", json={"name": "sp3", "protocol": "sparkplug",
                                               "endpoint": "mqtt://localhost:1883", "enabled": True}) as r:
        assert r.status == 400 and "would get Sparkplug rows (sp3) and other rows (m)" in (await r.json())["message"]
    async with http.put("/api/sources/m", json={"name": "m", "protocol": "mqtt", "endpoint": "mqtt://localhost:1883",
                                                "enabled": True, "target_table": "c.s.sparkplug_raw"}) as r:
        assert r.status == 400 and "would get Sparkplug rows (sp) and other rows (m)" in (await r.json())["message"]
    async with http.put("/api/sources/m", json={"name": "m", "protocol": "mqtt", "endpoint": "mqtt://localhost:1883",
                                                "enabled": True, "target_table": "bronze"}) as r:
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
