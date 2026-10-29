"""The bridge's error paths: a table that can't be set up, unknown sources, a failing Zerobus start."""
from __future__ import annotations

import pytest

from test_wal import bridge_config, fake_streams  # noqa: F401 - a fixture
from test_zerobus_restart import hosts, sparkplug_source  # noqa: F401 - a fixture
from sparkplug_connector.core import bridge as core_bridge


async def test_a_table_whose_streams_cant_be_set_up_gets_no_writer_and_its_host_stays_offline(
        tmp_path, monkeypatch, hosts, caplog):
    class Broken:
        def __init__(self, *args):
            raise RuntimeError("no Zerobus SDK")

    monkeypatch.setattr(core_bridge, "ZerobusRowStreams", Broken)
    cfg = bridge_config(tmp_path)
    cfg["sources"] = [sparkplug_source()]
    bridge = core_bridge.Bridge(cfg)
    await bridge.start()
    try:
        assert bridge.writers == {} and bridge.tables == {}
        assert "Failed to set up the ZeroBus target example.c.s.sparkplug_raw: no Zerobus SDK" in caplog.text
        assert await bridge.host_supervisors["sp"].tick() == "OFFLINE"     # nothing is being written
    finally:
        await bridge.stop()


async def test_editing_or_removing_an_unknown_source_is_refused(tmp_path):
    bridge = core_bridge.Bridge(bridge_config(tmp_path))
    with pytest.raises(ValueError, match="not found"):
        await bridge.update_source("nope", {"name": "nope"})
    with pytest.raises(ValueError, match="not found"):
        await bridge.remove_source("nope")
    await bridge.remove_source("sp")                         # a stopped source: from the configuration only
    assert bridge.sources == []


async def test_a_zerobus_start_that_fails_says_so(tmp_path, fake_streams, monkeypatch):
    bridge = core_bridge.Bridge(bridge_config(tmp_path))

    async def broken():
        raise RuntimeError("bad table")
    monkeypatch.setattr(bridge, "_open_targets", broken)
    result = await bridge.start_zerobus()
    assert result == {"status": "error", "message": "Failed to start ZeroBus: bad table"}
    assert not bridge.zerobus_enabled


async def test_a_refused_mqtt_login_is_the_source_s_error_until_it_connects(monkeypatch):
    """In an end-to-end test a source with no password was refused by the MQTT server while the page showed it
    Online (a primary host's state follows Zerobus). The bridge keeps the last connection error, since when the
    connection fails, until a connect succeeds; the page shows it as the source's Error."""
    import asyncio
    from test_per_target_queues import wait_for
    from test_web_sources import IdleClient

    class RefusedTwice(IdleClient):
        attempts = 0

        async def connect(self):
            RefusedTwice.attempts += 1
            if RefusedTwice.attempts <= 2:
                raise ConnectionRefusedError("[code:5] Connection Refused: not authorised.")
            self.is_connected = True                     # connected: returns, as SparkplugClient.connect does

        async def subscribe(self):
            pass

    monkeypatch.setattr(core_bridge, "create_protocol_client", lambda **kw: RefusedTwice(kw["config"]))
    bridge = core_bridge.Bridge({"sources": [{"name": "chariot", "protocol": "sparkplug", "endpoint": "mqtt://h:1883",
                                              "username": "pyconnector"}]})
    bridge.reconnect_initial_delay_sec = bridge.reconnect_max_delay_sec = 0.2
    await bridge.start()
    try:
        await wait_for(lambda: bridge.sparkplug_health()["chariot"].get("mqtt_last_error"), 5)
        h = bridge.sparkplug_health()["chariot"]
        assert "not authorised" in h["mqtt_last_error"] and h["mqtt_failing_since"] is not None
        first_since = h["mqtt_failing_since"]
        await wait_for(lambda: RefusedTwice.attempts >= 2, 5)
        assert bridge.sparkplug_health()["chariot"]["mqtt_failing_since"] == first_since     # failing since the first
        await wait_for(lambda: RefusedTwice.attempts >= 3 and bridge.clients["chariot"].is_connected, 5)
        await asyncio.sleep(0.05)
        h = bridge.sparkplug_health()["chariot"]
        assert h["mqtt_last_error"] is None and h["mqtt_failing_since"] is None
    finally:
        await bridge.stop()
