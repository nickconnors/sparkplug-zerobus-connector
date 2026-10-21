"""The one example configuration (unified_connector/config/config.yaml): every protocol's example source is
off, so the connector starts nothing and opens only the default table; a disabled source's table is opened
once it is enabled and started."""
from __future__ import annotations

import pathlib
import re

from test_confirmed_writer import FakeStreams
from test_web_sources import started  # noqa: F401  (fixture)
from unified_connector.core import unified_bridge
from unified_connector.core.config_loader import ConfigLoader

EXAMPLE = pathlib.Path(unified_bridge.__file__).resolve().parents[1] / "config" / "config.yaml"
ENV = {"ZEROBUS_ENDPOINT": "123.zerobus.region.cloud.databricks.com", "DATABRICKS_HOST": "https://example.cloud.databricks.com",
       "DATABRICKS_CLIENT_ID": "id", "DATABRICKS_CLIENT_SECRET": "secret"}


class FakeZeroBusClient:
    def __init__(self, cfg):
        z = cfg["zerobus"]
        t = z["target"]
        self.full_table_name = f"{t['catalog']}.{t['schema']}.{t['table']}"
        self.zerobus_endpoint, self.workspace_url = z["zerobus_endpoint"], z["workspace_host"]
        self.client_id, self.client_secret = z["auth"]["client_id"], z["auth"]["client_secret"]

    async def connect(self):
        pass

    async def close(self):
        pass

    def get_metrics(self):
        return {}


class FakeRowStreams:
    def __init__(self, *args):
        self.streams = FakeStreams()

    async def open(self):
        return await self.streams.open()


def example(monkeypatch, tmp_path) -> dict:
    for k, v in ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("HOME", str(tmp_path))      # the credential manager's directory
    monkeypatch.chdir(tmp_path)                    # spool and log paths
    monkeypatch.setattr(unified_bridge, "ZeroBusClient", FakeZeroBusClient)
    monkeypatch.setattr(unified_bridge, "ZerobusRowStreams", FakeRowStreams)
    return ConfigLoader(config_path=EXAMPLE).load()


def test_the_example_has_one_source_per_protocol_all_off_and_no_workspace_values():
    config = ConfigLoader(config_path=EXAMPLE).load_raw()
    assert sorted((s["protocol"], s["enabled"]) for s in config["sources"]) == [
        ("modbus", False), ("mqtt", False), ("opcua", False), ("sparkplug", False)]
    zerobus = config["zerobus"]
    assert zerobus["zerobus_endpoint"] == "${env:ZEROBUS_ENDPOINT}"
    assert zerobus["workspace_host"] == zerobus["default_target"]["workspace_host"] == "${env:DATABRICKS_HOST}"
    assert zerobus["auth"] == {"client_id": "${env:DATABRICKS_CLIENT_ID}", "client_secret": "${env:DATABRICKS_CLIENT_SECRET}"}
    assert "monitoring" not in config     # its health_check/prometheus ports were never served
    hosts = re.findall(r"[\w<>.-]+\.cloud\.databricks\.com", EXAMPLE.read_text())
    assert hosts and all("<" in h for h in hosts), hosts   # placeholders only, no real workspace


async def test_the_example_starts_no_source_and_opens_only_the_default_table(monkeypatch, tmp_path, started):  # noqa: F811
    bridge = unified_bridge.UnifiedBridge(example(monkeypatch, tmp_path))
    await bridge.start()
    try:
        assert started == [] and bridge.clients == {}
        assert list(bridge.zerobus_clients) == ["example.opcua.scada_data.opcua_events_bronze"]
        assert bridge.writers == {}     # before: the disabled sparkplug-example's table got a confirmed writer
    finally:
        await bridge.stop()


async def test_a_disabled_source_s_table_is_opened_when_it_is_enabled_and_started(monkeypatch, tmp_path, started):  # noqa: F811
    bridge = unified_bridge.UnifiedBridge(example(monkeypatch, tmp_path))
    await bridge.start()
    try:
        source = next(s for s in bridge.sources if s["name"] == "sparkplug-example")
        assert (await bridge.start_source("sparkplug-example"))["status"] == "error"   # disabled
        await bridge.update_source("sparkplug-example", {**source, "enabled": True})
        assert (await bridge.start_source("sparkplug-example"))["status"] == "ok"
        assert [name for name, _ in started] == ["sparkplug-example"]
        assert "example.bridge_stage_db.stage_db.sparkplug_raw" in bridge.writers
    finally:
        await bridge.stop()
