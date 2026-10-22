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
