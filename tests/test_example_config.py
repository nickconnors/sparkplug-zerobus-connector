"""The example configuration (sparkplug_connector/config/config.yaml): its one Sparkplug source is off, so the
connector starts nothing and opens no table; a disabled source's table is opened once it is enabled and
started."""
from __future__ import annotations

import pathlib
import re

from test_confirmed_writer import FakeStreams
from test_web_sources import started  # noqa: F401  (fixture)
from sparkplug_connector.core import bridge as core_bridge
from sparkplug_connector.core.config_loader import ConfigLoader

EXAMPLE = pathlib.Path(core_bridge.__file__).resolve().parents[1] / "config" / "config.yaml"
ENV = {"DATABRICKS_CLIENT_SECRET": "secret"}      # the secrets only; the rest is written into the file


class FakeRowStreams:
    def __init__(self, *args):
        self.streams = FakeStreams()

    async def open(self):
        return await self.streams.open()


def example(monkeypatch, tmp_path) -> dict:
    """The example as the README's step 2 edits it: the workspace, the Zerobus endpoint and the client id set."""
    for k, v in ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("HOME", str(tmp_path))      # the credential manager's directory
    monkeypatch.chdir(tmp_path)                    # log and write-ahead log paths
    monkeypatch.setattr(core_bridge, "ZerobusRowStreams", FakeRowStreams)
    config = ConfigLoader(config_path=EXAMPLE).load()
    config["zerobus"].update(enabled=True, zerobus_endpoint="123.zerobus.region.cloud.databricks.com")
    config["zerobus"]["default_target"]["workspace_host"] = "https://example.cloud.databricks.com"
    config["zerobus"]["auth"]["client_id"] = "id"
    return config


def test_the_example_has_one_sparkplug_source_off_and_no_workspace_values_or_secrets():
    """Only the secrets come from the environment; the endpoints, the workspace, the client id and the MQTT user are
    written into the file (empty in the example), once each."""
    config = ConfigLoader(config_path=EXAMPLE).load_raw()
    assert [(s["protocol"], s["enabled"]) for s in config["sources"]] == [("sparkplug", False)]
    source = config["sources"][0]
    assert (source["endpoint"], source["username"], source["password"]) == ("", "", "${env:SPARKPLUG_MQTT_PASSWORD:}")
    zerobus = config["zerobus"]
    assert zerobus["zerobus_endpoint"] == zerobus["default_target"]["workspace_host"] == ""
    assert zerobus["enabled"] is False          # the first start tries no table: switched on once it is set up
    assert "workspace_host" not in zerobus                  # the default target's is the one the bridge reads
    assert zerobus["auth"] == {"client_id": "", "client_secret": "${env:DATABRICKS_CLIENT_SECRET}"}
    assert config["web_ui"]["authentication"]["oauth"]["workspace_host"] == ""
    assert set(re.findall(r"\$\{env:([A-Z_]+)", EXAMPLE.read_text())) == {"DATABRICKS_CLIENT_SECRET",
                                                                         "SPARKPLUG_MQTT_PASSWORD"}
    for gone in ("monitoring", "discovery", "normalization", "numeric_compression"):
        assert gone not in config
    assert "disk_spool" not in config["backpressure"] and "confirmed_writes" not in zerobus
    hosts = re.findall(r"[\w<>.-]+\.cloud\.databricks\.com", EXAMPLE.read_text())
    assert hosts and all("<" in h for h in hosts), hosts   # placeholders only, no real workspace


async def test_the_example_starts_no_source_and_opens_no_table(monkeypatch, tmp_path, started):  # noqa: F811
    bridge = core_bridge.Bridge(example(monkeypatch, tmp_path))
    await bridge.start()
    try:
        assert started == [] and bridge.clients == {}
        assert bridge.tables == {} and bridge.writers == {}
    finally:
        await bridge.stop()


async def test_a_disabled_source_s_table_is_opened_when_it_is_enabled_and_started(monkeypatch, tmp_path, started):  # noqa: F811
    bridge = core_bridge.Bridge(example(monkeypatch, tmp_path))
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
