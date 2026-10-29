"""The bridge's Stop, Start and Restart from the web page (core/lifecycle.py): a start after a stop is a new bridge
from the configuration file (Bridge.stop() is for good), so a restart applies the saved settings; rows not written
survive a restart through the write-ahead log; the actions run one at a time; the connector's shutdown cancels a
start in progress and stops the bridge once."""
from __future__ import annotations

import asyncio
import socket

import aiohttp
import pytest
import yaml

from test_per_target_queues import rows, wait_for
from test_wal import bridge_config, fake_streams, ids  # noqa: F401 - fake_streams: a fixture
from test_web_sources import CONFIG, started  # noqa: F401 - started: a fixture (sources that never connect)
from test_zerobus_restart import acked, written  # noqa: F401 - acked: a fixture
from sparkplug_connector.core import bridge as core_bridge
from sparkplug_connector.core.config_loader import ConfigLoader
from sparkplug_connector.core.lifecycle import BridgeLifecycle
from sparkplug_connector.web.web_server import WebServer


@pytest.fixture
async def site(tmp_path, monkeypatch, started):
    """The web server and a running bridge on a config file, as ``python -m sparkplug_connector --config`` makes them."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("SPTEST_PASSWORD", "s3cret")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    path = tmp_path / "connector.yaml"
    path.write_text(CONFIG.format(port=port))
    loader = ConfigLoader(config_path=path)
    config = loader.load()
    lifecycle = BridgeLifecycle(core_bridge.Bridge(config), load_config=loader.load)
    server = WebServer(config=config, bridge=lifecycle.bridge, config_path=path, lifecycle=lifecycle)
    await server.start()
    await lifecycle.start()
    async with aiohttp.ClientSession(f"http://127.0.0.1:{port}") as http:
        yield http, path, server
    await lifecycle.shutdown()
    await server.stop()


async def status(http) -> dict:
    async with http.get("/api/status") as r:
        return await r.json()


async def post(http, path) -> tuple[int, dict]:
    async with http.post(path) as r:
        return r.status, await r.json()


async def test_stop_then_start_runs_the_bridge_again_from_the_file(site, started):
    http, path, server = site
    first = server.bridge
    s = await status(http)
    assert s["bridge_state"] == "running" and sorted(x["name"] for x in s["sources"] if x["running"]) == ["m", "sp"]

    assert (await post(http, "/api/bridge/stop"))[0] == 200
    s = await status(http)
    assert (s["bridge_state"], s["bridge_running"], s["active_sources"]) == ("stopped", False, 0)

    raw = yaml.safe_load(path.read_text())
    raw["sources"][1]["enabled"] = False                    # m, edited in the file while stopped
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    assert (await post(http, "/api/bridge/start"))[0] == 200
    await wait_for(lambda: server.lifecycle.state == "running" and "sp" in server.bridge.clients, 5)
    s = await status(http)
    assert server.bridge is not first and not first.running
    assert [x["name"] for x in s["sources"] if x["running"]] == ["sp"]
    assert [name for name, _ in started] == ["sp", "m", "sp"]


async def test_a_restart_applies_the_saved_settings_and_clears_the_banner(site):
    http, _, server = site
    async with http.put("/api/settings", json={"values": {"zerobus.drain_timeout_seconds": 3}}) as r:
        assert r.status == 200, await r.text()
    assert (await status(http))["restart_required"] is True
    assert server.bridge.drain_timeout_s == 15
    assert (await post(http, "/api/bridge/restart"))[0] == 200
    await wait_for(lambda: server.lifecycle.state == "running" and server.bridge.clients, 5)
    s = await status(http)
    assert s["restart_required"] is False and server.bridge.drain_timeout_s == 3


async def test_an_action_while_another_runs_is_refused(site):
    http, _, server = site
    release = asyncio.Event()
    stop = server.bridge.stop

    async def slow_stop():
        await release.wait()
        await stop()
    server.bridge.stop = slow_stop
    assert (await post(http, "/api/bridge/restart"))[0] == 200
    await wait_for(lambda: server.lifecycle.state == "stopping", 2)
    code, body = await post(http, "/api/bridge/stop")
    assert code == 409 and body["state"] == "stopping"
    assert (await status(http))["bridge_state"] == "stopping"
    release.set()
    await wait_for(lambda: server.lifecycle.state == "running", 5)


async def test_rows_not_written_before_a_stop_are_written_once_by_the_next_bridge(tmp_path, acked):
    """Zerobus never acknowledges the first rows; the stop gives up draining them (they stay in the write-ahead log),
    the new bridge sends them first, then the rows that arrive after its start."""
    config = bridge_config(tmp_path)
    config["zerobus"]["drain_timeout_seconds"] = 0.3
    lifecycle = BridgeLifecycle(core_bridge.Bridge(config), load_config=lambda: config)
    await lifecycle.bridge.start_infra()                    # the table's writer, no source
    acked.fail_waits = 10**6
    first = rows("A", 3)
    for r in first:
        lifecycle.bridge._on_record(r)
    await wait_for(lambda: acked.ingested, 5)
    await lifecycle.stop()
    acked.fail_waits = 0
    await lifecycle.start()
    later = rows("A", 1)
    lifecycle.bridge._on_record(later[0])
    await wait_for(lambda: len(written(acked)) >= 4, 5)
    await lifecycle.shutdown()
    assert written(acked) == [r.msg_id for r in first + later]


class FakeBridge:
    """Records its start and stop in ``events``; a start or stop waits for ``gate`` when one is set."""

    def __init__(self, events: list, gate: asyncio.Event | None = None):
        self.events, self.gate = events, gate
        self.running = self._starting = False

    async def start(self):
        self.events.append(("start", id(self)))
        self._starting = True
        if self.gate is not None:
            await self.gate.wait()
        await asyncio.sleep(0.01)
        self.running, self._starting = True, False
        self.events.append(("started", id(self)))

    async def stop(self):
        self.events.append(("stop", id(self)))
        await asyncio.sleep(0.01)
        self.running = False
        self.events.append(("stopped", id(self)))


async def test_restarts_run_one_after_the_other_each_with_a_new_bridge():
    events: list = []
    made = []

    def make(config):
        made.append(FakeBridge(events))
        return made[-1]
    lifecycle = BridgeLifecycle(make({}), load_config=dict, make_bridge=make)
    seen = []
    lifecycle.add_listener(lambda bridge, config: seen.append(bridge))
    await lifecycle.start()
    await asyncio.gather(lifecycle.submit("restart"), lifecycle.submit("restart"))
    kinds = [e[0] for e in events]
    assert kinds == ["start", "started", "stop", "stopped", "start", "started", "stop", "stopped", "start", "started"]
    assert [e[1] for e in events[::2]] == [id(made[0]), id(made[0]), id(made[1]), id(made[1]), id(made[2])]
    assert seen == made[1:] and lifecycle.bridge is made[2] and lifecycle.state == "running"


async def test_the_shutdown_cancels_a_start_in_progress_then_stops_once():
    events: list = []
    lifecycle = BridgeLifecycle(FakeBridge(events, gate=asyncio.Event()), load_config=dict)
    task = lifecycle.submit("start")
    await wait_for(lambda: events, 2)                       # the bridge's start is running (waiting at its gate)
    assert lifecycle.state == "starting"
    await lifecycle.shutdown()
    assert task.cancelled() and [e[0] for e in events] == ["start", "stop", "stopped"]
    await lifecycle.shutdown()                              # already stopped: nothing more
    assert [e[0] for e in events] == ["start", "stop", "stopped"] and lifecycle.state == "stopped"


# ----------------------------------------------------------------------------- review findings (2026-10-28)

async def test_zerobus_and_source_actions_wait_for_a_running_bridge(site):
    """A stopped bridge is for good: the Zerobus page's Stop/Start or a source's Start would revive writers on it
    (on its closed write-ahead logs) that nothing stops again. Refused until the bridge is started."""
    http, _, server = site
    assert (await post(http, "/api/bridge/stop"))[0] == 200
    for path in ("/api/zerobus/stop", "/api/zerobus/start", "/api/sources/sp/start"):
        code, body = await post(http, path)
        assert code == 409 and "stopped" in body["message"], (path, code, body)
    assert server.bridge.writer_tasks == {} and server.bridge.clients == {}


async def test_a_stopped_bridge_opens_no_writer_again(tmp_path, acked):
    config = bridge_config(tmp_path)
    lifecycle = BridgeLifecycle(core_bridge.Bridge(config), load_config=lambda: config)
    await lifecycle.bridge.start_infra()
    await lifecycle.stop()
    bridge = lifecycle.bridge
    assert (await bridge.stop_zerobus())["status"] == "error"
    assert (await bridge.start_zerobus())["status"] == "error"
    assert all(t.done() for t in bridge.writer_tasks.values())


async def test_an_action_is_busy_from_the_moment_it_is_submitted():
    """A double click: two requests in one event-loop turn; the second must see the first as busy, though its task
    hasn't taken the lock yet."""
    events: list = []
    lifecycle = BridgeLifecycle(FakeBridge(events), load_config=dict, make_bridge=lambda c: FakeBridge(events))
    await lifecycle.start()
    task = lifecycle.submit("restart")
    assert lifecycle.busy and lifecycle.state == "stopping"     # nothing has run yet
    await task
    assert not lifecycle.busy and lifecycle.state == "running"


async def test_the_answer_to_start_and_restart_has_the_new_state(site):
    http, _, _ = site
    code, body = await post(http, "/api/bridge/restart")
    assert code == 200 and body["state"] == "stopping"
    await wait_for(lambda: site[2].lifecycle.state == "running", 5)
    assert (await post(http, "/api/bridge/stop"))[0] == 200
    code, body = await post(http, "/api/bridge/start")
    assert code == 200 and body["state"] == "starting"


async def test_the_shutdown_during_a_restart_s_drain_starts_no_new_bridge():
    """SIGTERM while a restart drains: the restart's stop finishes, its start doesn't run (no sources connecting
    and going ONLINE just to be stopped again)."""
    events: list = []
    made = []

    def make(config):
        made.append(FakeBridge(events))
        return made[-1]
    first = FakeBridge(events)
    lifecycle = BridgeLifecycle(first, load_config=dict, make_bridge=make)
    await lifecycle.start()
    gate = asyncio.Event()
    stop = first.stop

    async def slow_stop():
        await gate.wait()
        await stop()
    first.stop = slow_stop
    lifecycle.submit("restart")
    await wait_for(lambda: lifecycle.state == "stopping", 2)
    shutting = asyncio.create_task(lifecycle.shutdown())
    await asyncio.sleep(0.05)
    gate.set()
    await shutting
    assert made == [] and [e[0] for e in events] == ["start", "started", "stop", "stopped"]
    assert lifecycle.state == "stopped"


async def test_the_status_says_when_the_bridge_started_and_a_restart_starts_again(site):
    """The counters count from the bridge's start (each Start or Restart is a new bridge): the page says since when."""
    import time
    http, _, server = site
    first = (await status(http))["bridge_started_at"]
    assert abs(first / 1000 - time.time()) < 30
    await asyncio.sleep(0.01)
    assert (await post(http, "/api/bridge/restart"))[0] == 200
    await wait_for(lambda: server.lifecycle.state == "running" and server.bridge.running, 5)
    assert (await status(http))["bridge_started_at"] > first
