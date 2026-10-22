"""The example configuration (unified_connector/config/config.yaml): its one Sparkplug source is off, so the
connector starts nothing and opens no table; a disabled source's table is opened once it is enabled and
started."""
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


class FakeRowStreams:
    def __init__(self, *args):
        self.streams = FakeStreams()

    async def open(self):
        return await self.streams.open()


def example(monkeypatch, tmp_path) -> dict:
    for k, v in ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("HOME", str(tmp_path))      # the credential manager's directory
    monkeypatch.chdir(tmp_path)                    # log and write-ahead log paths
    monkeypatch.setattr(unified_bridge, "ZerobusRowStreams", FakeRowStreams)
    return ConfigLoader(config_path=EXAMPLE).load()


def test_the_example_has_one_sparkplug_source_off_and_no_workspace_values_or_secrets():
    config = ConfigLoader(config_path=EXAMPLE).load_raw()
    assert [(s["protocol"], s["enabled"]) for s in config["sources"]] == [("sparkplug", False)]
    source = config["sources"][0]
    assert source["endpoint"].startswith("${env:SPARKPLUG_MQTT_ENDPOINT")
    assert source["username"].startswith("${env:") and source["password"].startswith("${env:")
    zerobus = config["zerobus"]
    assert zerobus["zerobus_endpoint"] == "${env:ZEROBUS_ENDPOINT}"
    assert zerobus["workspace_host"] == zerobus["default_target"]["workspace_host"] == "${env:DATABRICKS_HOST}"
    assert zerobus["auth"] == {"client_id": "${env:DATABRICKS_CLIENT_ID}", "client_secret": "${env:DATABRICKS_CLIENT_SECRET}"}
    for gone in ("monitoring", "discovery", "normalization", "numeric_compression"):
        assert gone not in config
    assert "disk_spool" not in config["backpressure"] and "confirmed_writes" not in zerobus
    hosts = re.findall(r"[\w<>.-]+\.cloud\.databricks\.com", EXAMPLE.read_text())
    assert hosts and all("<" in h for h in hosts), hosts   # placeholders only, no real workspace


async def test_the_example_starts_no_source_and_opens_no_table(monkeypatch, tmp_path, started):  # noqa: F811
    bridge = unified_bridge.UnifiedBridge(example(monkeypatch, tmp_path))
    await bridge.start()
    try:
        assert started == [] and bridge.clients == {}
        assert bridge.tables == {} and bridge.writers == {}
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
