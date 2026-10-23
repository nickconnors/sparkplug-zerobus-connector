"""The web page's source add/edit/delete: edits keep what the form doesn't show, saves go to the --config
file with its placeholders, the API masks passwords, NBIRTH procedure names are checked; a stopped Sparkplug
source leaves no NBIRTH caller behind."""
from __future__ import annotations

import asyncio
import pathlib
import socket

import aiohttp
import pytest
import yaml

from test_confirmed_writer import FakeStreams
from sparkplug_connector.core import bridge as core_bridge
from sparkplug_connector.core.config_loader import ConfigLoader
from sparkplug_connector.protocols.sparkplug_client import SparkplugClient
from sparkplug_connector.web.web_server import WebServer

PACKAGE_CONFIG = pathlib.Path(core_bridge.__file__).resolve().parents[1] / "config" / "config.yaml"

CONFIG = """\
# Header: the environment variables this file reads (kept on save)
# SPTEST_PASSWORD, SPTEST_ENDPOINT, SPTEST_ENS
web_ui:
  enabled: true
  host: 127.0.0.1
  port: {port}
  authentication:
    enabled: false
zerobus:
  enabled: false
sources:
- name: sp
  protocol: sparkplug
  enabled: true
  endpoint: '${{env:SPTEST_ENDPOINT:mqtt://localhost:1883}}'
  target_table: c.s.sparkplug_raw
  client_id: pyc
  username: sparkplug-user
  password: '${{env:SPTEST_PASSWORD}}'
  topics: ['spBv1.0/#']
  primary_host_id: databricks-zerobus
  offline_after_seconds: 30
  online_after_seconds: 30
  edge_node_schemas:
    enabled: '${{env:SPTEST_ENS:false}}'
    warehouse_id: ''
    procedures: [a.b.create_edge_node_schema]
    schema_prefix: nb
- name: m
  protocol: sparkplug
  enabled: true
  endpoint: mqtt://localhost:1883
  username: viewer
  password: '${{env:SPTEST_PASSWORD}}'
  security:
    password: '${{env:SPTEST_PASSWORD}}'
"""


class IdleClient:
    """A protocol client that connects forever (the tests look at configuration, not data)."""

    def __init__(self, config):
        self.config = config
        self.primary_host_id = config.get("primary_host_id") or None
        self.offline_after_seconds = config.get("offline_after_seconds", 30)
        self.online_after_seconds = config.get("online_after_seconds", 30)
        self.online_allowed = asyncio.Event()
        self.is_connected = False

    async def go_offline(self):
        pass

    async def close(self):
        pass

    async def connect(self):
        await asyncio.Event().wait()

    async def stop(self):
        pass

    async def disconnect(self):
        pass

    def edge_nodes(self):
        return getattr(self, "nodes", [])


@pytest.fixture
def started(monkeypatch):
    """Clients the bridge started: (name, config)."""
    started = []

    def factory(protocol_type, source_name, endpoint, config, on_record, on_stats):
        started.append((source_name, dict(config)))
        return IdleClient(config)

    monkeypatch.setattr(core_bridge, "create_protocol_client", factory)
    return started


@pytest.fixture
async def page(tmp_path, monkeypatch, started):
    """The connector's web server and bridge on a config file, as ``python -m sparkplug_connector --config``."""
    monkeypatch.setenv("HOME", str(tmp_path))                 # the credential manager's directory
    monkeypatch.setenv("SPTEST_PASSWORD", "s3cret")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    path = tmp_path / "connector.yaml"
    path.write_text(CONFIG.format(port=port))
    config = ConfigLoader(config_path=path).load()
    bridge = core_bridge.Bridge(config)
    await bridge.start()
    server = WebServer(config=config, bridge=bridge, config_path=path)
    await server.start()
    async with aiohttp.ClientSession(f"http://127.0.0.1:{port}") as http:
        yield http, path, bridge
    await server.stop()
    await bridge.stop()


def saved(path) -> dict:
    return {s["name"]: s for s in yaml.safe_load(path.read_text())["sources"]}


def running(bridge, name) -> dict:
    return next(s for s in bridge.sources if s["name"] == name)


EDIT = {"name": "sp", "protocol": "sparkplug", "endpoint": "mqtt://localhost:1883", "enabled": True,
        "site": "", "area": "", "line": "", "equipment": "",
        "client_id": "pyc", "primary_host_id": "", "state_format": "v3", "offline_after_seconds": 45,
        "online_after_seconds": "",
        "edge_node_schemas": {"enabled": False, "warehouse_id": "wh-1", "procedures": ["a.b.create_edge_node_schema"],
                              "schema_prefix": "nb"}}


async def test_an_edit_keeps_unshown_fields_and_saves_placeholders_to_the_config_file(page, started):
    http, path, bridge = page
    package_config = PACKAGE_CONFIG.read_bytes()
    async with http.put("/api/sources/sp", json=EDIT) as r:
        assert r.status == 200, await r.text()

    sp = saved(path)["sp"]
    assert sp["username"] == "sparkplug-user" and sp["topics"] == ["spBv1.0/#"]           # not in the form: kept
    assert sp["password"] == "${env:SPTEST_PASSWORD}"                                  # placeholder, not the value
    assert sp["endpoint"] == "${env:SPTEST_ENDPOINT:mqtt://localhost:1883}"            # unchanged: file's spelling
    assert "primary_host_id" not in sp and "online_after_seconds" not in sp             # cleared
    assert sp["offline_after_seconds"] == 45 and sp["state_format"] == "v3"
    assert sp["edge_node_schemas"] == {"enabled": "${env:SPTEST_ENS:false}", "warehouse_id": "wh-1",
                                       "procedures": ["a.b.create_edge_node_schema"], "schema_prefix": "nb"}
    assert saved(path)["m"]["password"] == "${env:SPTEST_PASSWORD}"
    text = path.read_text()
    assert "s3cret" not in text
    assert text.startswith("# Header: the environment variables this file reads (kept on save)\n# SPTEST_PASSWORD")
    assert PACKAGE_CONFIG.read_bytes() == package_config                                # not the package's example

    name, config = started[-1]                                                          # restarted with the edit
    assert name == "sp" and config["password"] == "s3cret" and config["offline_after_seconds"] == 45
    assert "primary_host_id" not in config
    assert running(bridge, "sp")["password"] == "s3cret"


async def test_the_api_masks_passwords_and_secrets(page):
    http, _, _ = page
    async with http.get("/api/sources") as r:
        sources = {s["name"]: s for s in (await r.json())["sources"]}
    assert sources["sp"]["password"] == "********" and sources["m"]["security"]["password"] == "********"
    assert sources["sp"]["username"] == "sparkplug-user"
    assert "s3cret" not in str(sources)


async def test_a_sparkplug_source_added_from_the_page_keeps_its_settings(page, started):
    http, path, bridge = page
    form = {"name": "sp2", "protocol": "sparkplug", "endpoint": "mqtt://chariot:1883", "enabled": True,
            "target_table": "c.s.sparkplug_raw",
            "primary_host_id": "h", "state_format": "v2", "offline_after_seconds": 10,
            "edge_node_schemas": {"enabled": True, "warehouse_id": "w", "procedures": ["x.y.on_nbirth"], "schema_prefix": ""}}
    async with http.post("/api/sources", json=form) as r:
        assert r.status == 200, await r.text()
    sp2 = saved(path)["sp2"]
    assert sp2["primary_host_id"] == "h" and sp2["state_format"] == "v2" and sp2["offline_after_seconds"] == 10
    assert sp2["edge_node_schemas"]["procedures"] == ["x.y.on_nbirth"]
    assert started[-1][0] == "sp2" and started[-1][1]["primary_host_id"] == "h"
    assert set(saved(path)) == {"sp", "m", "sp2"}


@pytest.mark.parametrize("procedures, warehouse, message", [
    (["x.y.p; DROP TABLE t"], "wh-1", "catalog.schema.procedure"),
    (["create_edge_node_schema"], "wh-1", "catalog.schema.procedure"),
    ([], "wh-1", "at least one"),                                  # switched on, nothing to call
    (["x.y.p"], "", "SQL warehouse ID"),                           # switched on, nowhere to run it
])
async def test_an_nbirth_procedure_that_cant_be_called_is_refused(page, started, procedures, warehouse, message):
    http, path, bridge = page
    before, count = path.read_text(), len(started)
    edit = {**EDIT, "edge_node_schemas": {**EDIT["edge_node_schemas"], "enabled": True, "procedures": procedures,
                                          "warehouse_id": warehouse}}
    async with http.put("/api/sources/sp", json=edit) as r:
        assert r.status == 400 and message in await r.text()
    add = {**edit, "name": "sp3"}
    async with http.post("/api/sources", json=add) as r:
        assert r.status == 400 and message in await r.text()
    assert path.read_text() == before and len(started) == count


async def test_a_stopped_source_can_be_edited_and_deleted(page, started):
    http, path, bridge = page
    await bridge.stop_source("sp")
    count = len(started)
    async with http.put("/api/sources/sp", json=EDIT) as r:
        assert r.status == 200, await r.text()
    assert len(started) == count                                   # stays stopped
    assert running(bridge, "sp")["offline_after_seconds"] == 45 and running(bridge, "sp")["password"] == "s3cret"
    async with http.delete("/api/sources/sp") as r:
        assert r.status == 200, await r.text()
    assert set(saved(path)) == {"m"} and [s["name"] for s in bridge.sources] == ["m"]


def confirmed_bridge(monkeypatch, source):
    """A bridge with one source and confirmed writes to fake Zerobus streams."""
    streams = FakeStreams()

    class FakeRowStreams:
        def __init__(self, *args):
            pass

        async def open(self):
            return await streams.open()

    monkeypatch.setattr(core_bridge, "ZerobusRowStreams", FakeRowStreams)
    return core_bridge.Bridge({
        "sources": [source],
        "zerobus": {"enabled": True, "zerobus_endpoint": "https://example.zerobus",
                    "auth": {"client_id": "id", "client_secret": "secret"},
                    "default_target": {"workspace_host": "https://example.cloud.databricks.com", "catalog": "c",
                                       "schema": "s", "table": "t"}},
        "backpressure": {},
    })


@pytest.mark.parametrize("settings", [{"procedures": ["a.b.p; DROP TABLE t"]}, {"node_schemas": ["a.b"]},
                                      {"procedures": ["a.b.p"], "warehouse_id": None},
                                      {"procedures": ["a.b.p"], "warehouse_id": " "}])
async def test_a_bad_nbirth_procedure_in_the_file_leaves_the_source_running_without_calls(monkeypatch, started,
                                                                                          settings):
    bridge = confirmed_bridge(monkeypatch, {
        "name": "sp", "protocol": "sparkplug", "endpoint": "mqtt://localhost:1883",
        "edge_node_schemas": {"enabled": True, "warehouse_id": "w", **settings}})
    await bridge.start()
    try:
        assert "sp" in bridge.client_tasks and bridge.birth_notifiers == {}
        assert next(iter(bridge.writers.values()))._listeners == []
    finally:
        await bridge.stop()


async def test_stopping_a_sparkplug_source_takes_its_nbirth_caller_off_the_writer(monkeypatch, started):
    bridge = confirmed_bridge(monkeypatch, {
        "name": "sp", "protocol": "sparkplug", "endpoint": "mqtt://localhost:1883",
        "edge_node_schemas": {"enabled": True, "warehouse_id": "w", "procedures": ["a.b.create_edge_node_schema"],
                              "schema_prefix": ""}})
    await bridge.start()
    try:
        writer = next(iter(bridge.writers.values()))
        assert len(writer._listeners) == 1
        assert bridge.birth_notifiers["sp"]._procedures == ["a.b.create_edge_node_schema"]
        assert bridge.birth_notifiers["sp"]._prefix == "nb"                      # empty: the default
        await bridge.stop_source("sp")
        assert writer._listeners == [] and bridge.birth_notifiers == {}
        await bridge.start_source("sp")
        assert len(writer._listeners) == 1                         # one caller, not two
    finally:
        await bridge.stop()


async def test_saving_the_zerobus_settings_goes_to_the_started_file_with_its_placeholders(page, started):
    """As the source changes: into the file the connector was started with (not the package's config.yaml,
    whose sources then replaced the running ones), ${env:...} kept (not the secret it resolves to)."""
    http, path, bridge = page
    default = pathlib.Path(core_bridge.__file__).parent.parent / "config" / "config.yaml"
    before = default.read_bytes()
    try:
        async with http.post("/api/zerobus/config", json={"batch": {"max_records": 250}}) as r:
            assert r.status == 200, await r.text()
    finally:
        changed = default.read_bytes() != before
        default.write_bytes(before)
    assert not changed
    text = path.read_text()
    assert yaml.safe_load(text)["zerobus"]["batch"]["max_records"] == 250
    assert "s3cret" not in text and text.count("${env:SPTEST_PASSWORD}") == 3
    assert text.startswith("# Header: the environment variables this file reads")
    assert [s["name"] for s in bridge.sources] == ["sp", "m"]


async def test_the_page_refuses_a_state_format_or_seconds_the_source_cant_use(page, started):
    http, path, bridge = page
    before, sp = path.read_text(), dict(running(bridge, "sp"))
    for field, value in [("state_format", "v4"), ("offline_after_seconds", "soon"), ("online_after_seconds", -1),
                         ("sequence_handling", "drop"), ("sequence_reordering_timeout", 0),
                         ("sequence_reordering_timeout", "soon"), ("block_auto_rebirth", "maybe"),
                         ("block_auto_rebirth", True)]:                # needs keep_all
        async with http.put("/api/sources/sp", json={"name": "sp", "protocol": "sparkplug", "endpoint": "mqtt://localhost:1883",
                                                     "enabled": True, field: value}) as r:
            assert r.status == 400, (field, await r.text())
    assert path.read_text() == before and running(bridge, "sp") == sp


async def test_the_page_saves_the_sequence_settings_and_the_source_uses_them(page, started):
    http, path, bridge = page
    async with http.put("/api/sources/sp", json={"name": "sp", "protocol": "sparkplug", "endpoint": "mqtt://localhost:1883",
                                                 "enabled": True, "sequence_handling": "keep_all",
                                                 "sequence_reordering_timeout": "2000", "block_auto_rebirth": True}) as r:
        assert r.status == 200, await r.text()
    saved = next(x for x in yaml.safe_load(path.read_text())["sources"] if x["name"] == "sp")
    assert (saved["sequence_handling"], saved["sequence_reordering_timeout"], saved["block_auto_rebirth"]) == ("keep_all", 2000, True)
    used = bridge.clients["sp"].config                              # what the restarted source was given
    assert (used["sequence_handling"], used["sequence_reordering_timeout"], used["block_auto_rebirth"]) == ("keep_all", 2000, True)
    seq = SparkplugClient("sp", "mqtt://localhost:1883", used, on_record=lambda r: None).sequencer
    assert (seq.mode, seq.timeout_s, seq.block_auto_rebirth) == ("keep_all", 2.0, True)


async def test_a_source_that_fails_to_start_with_its_new_settings_runs_on_with_the_old(page, started, monkeypatch):
    http, path, bridge = page
    factory = core_bridge.create_protocol_client

    def failing(protocol_type, source_name, endpoint, config, on_record, on_stats):
        if endpoint == "mqtt://broken:1883":
            raise ValueError("can't use that endpoint")
        return factory(protocol_type, source_name, endpoint, config, on_record, on_stats)
    monkeypatch.setattr(core_bridge, "create_protocol_client", failing)
    old = dict(running(bridge, "m"))
    with pytest.raises(ValueError, match="can't use that endpoint"):
        await bridge.update_source("m", {**old, "endpoint": "mqtt://broken:1883"})
    assert running(bridge, "m")["endpoint"] == old["endpoint"] and "m" in bridge.clients


async def test_add_and_edit_errors_are_reported_and_nothing_is_saved(page, started, monkeypatch):
    http, path, bridge = page
    before = path.read_text()
    form = {"name": "sp", "protocol": "sparkplug", "endpoint": "mqtt://localhost:1883", "enabled": True}
    async with http.post("/api/sources", json=form) as r:              # running already
        assert r.status == 500 and "already exists" in (await r.json())["message"]
    async with http.put("/api/sources/bad%20name", json=form) as r:
        assert r.status == 400 and "Invalid source name" in (await r.json())["message"]
    async with http.put("/api/sources/nope", json={**form, "name": "nope"}) as r:
        assert r.status == 404

    async def broken(name, config):
        raise RuntimeError("restart failed")
    monkeypatch.setattr(bridge, "update_source", broken)
    async with http.put("/api/sources/sp", json=form) as r:
        assert r.status == 500 and (await r.json())["message"] == "restart failed"
    assert path.read_text() == before


@pytest.mark.parametrize("master_password", ["", "a-master-password"])
async def test_saving_zerobus_credentials_never_writes_the_secret_to_the_file(page, started, monkeypatch, master_password):
    http, path, bridge = page
    if master_password:
        monkeypatch.setenv("CONNECTOR_MASTER_PASSWORD", master_password)
    else:
        monkeypatch.delenv("CONNECTOR_MASTER_PASSWORD", raising=False)
    async with http.post("/api/zerobus/config", json={"auth": {"client_id": "new-id", "client_secret": "new-secret"}}) as r:
        assert r.status == 200, await r.text()
        body = await r.json()
    text = path.read_text()
    auth = yaml.safe_load(text)["zerobus"]["auth"]
    assert "new-secret" not in text and auth["client_secret"] == "${credential:zerobus.client_secret}"
    if master_password:                                     # both in the encrypted credential store
        assert "warning" not in body and auth["client_id"] == "${credential:zerobus.client_id}"
        assert bridge.zerobus_config["auth"] == {"client_id": "new-id", "client_secret": "new-secret"}
    else:                                                   # no store: the id in the file, the secret nowhere
        assert "NOT stored" in body["warning"] and auth["client_id"] == "new-id"
    async with http.get("/api/zerobus/config") as r:
        shown = (await r.json())["auth"]
    assert "new-secret" not in str(shown)


@pytest.mark.parametrize("endpoint", ["https://localhost", "localhost"])
async def test_the_zerobus_diagnostics_check_the_endpoint_s_host_with_or_without_https(page, started, endpoint):
    http, path, bridge = page
    async with http.post("/api/zerobus/config", json={"zerobus_endpoint": endpoint}) as r:
        assert r.status == 200, await r.text()
    async with http.get("/api/zerobus/diagnostics") as r:
        checks = (await r.json())["checks"]
    assert checks["zerobus_dns_ok"] is True, checks           # "https://localhost" itself isn't a host name


async def test_the_edge_nodes_api_lists_each_sources_edge_nodes_and_devices_with_their_times(page):
    http, path, bridge = page
    bridge.clients["sp"].nodes = [{"group_id": "G", "edge_node_id": "E", "online": False, "since": 1790696925611,
                                   "changed_by": "NDEATH", "bdseq": 2,
                                   "devices": [{"device_id": "D", "online": False, "since": 1790696925011,
                                                "changed_by": "DDEATH"}]}]
    async with http.get("/api/sparkplug/edge-nodes") as r:
        assert r.status == 200
        body = await r.json()
    assert body["m"] == []
    node = body["sp"][0]
    assert (node["online"], node["since"], node["since_iso"], node["changed_by"]) == \
        (False, 1790696925611, "2026-09-29T15:48:45.611Z", "NDEATH")
    assert node["devices"][0]["since_iso"] == "2026-09-29T15:48:45.011Z"
