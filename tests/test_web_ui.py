"""The web page's server side: the pages and their files, the strict CSP, same origin only, the Settings API, the
certificate store and its use by the sources, and the source fields the Sources dialog edits (username, password,
keep-alive, topics, TLS files)."""
from __future__ import annotations

import contextlib
import os
import re
import stat

import aiohttp
import pytest
import yaml

from test_web_sources import page, running, saved, started  # noqa: F401 - page, started: fixtures
from sparkplug_connector.core.cert_store import CertStore, resolve_tls_paths, used_by
from sparkplug_connector.web.web_server import UI_DIR, UI_PAGES

SP = {"name": "sp", "protocol": "sparkplug", "endpoint": "mqtt://localhost:1883"}


# ----------------------------------------------------------------------------- pages and files

async def test_every_page_is_served_with_its_files(page):
    http, _, _ = page
    for name in UI_PAGES:
        async with http.get("/" if name == "index" else f"/{name}.html") as r:
            assert r.status == 200 and r.content_type == "text/html", name
            html = await r.text()
            csp = r.headers["Content-Security-Policy"]
        assert "<main x-data=" in html and "unsafe" not in csp and "script-src 'self';" in csp
        assert not re.search(r"<script>(?!\s*</script>)", html) and " style=" not in html      # nothing inline
        for ref in re.findall(r'(?:src|href)="(/(?:js|css|vendor|img)/[^"]+)"', html):
            async with http.get(ref) as asset:
                assert asset.status == 200, (name, ref)
    async with http.get("/nope.html") as r:
        assert r.status == 404
    async with http.get("/static/login.html") as r:
        html = await r.text()
        assert r.status == 200 and "<script>" not in html and 'href="/login"' in html


def test_every_page_script_and_component_exists():
    """Each page names its component (x-data) and the script that registers it."""
    for name in UI_PAGES:
        html = (UI_DIR / f"{name}.html").read_text()
        component = re.search(r'<main x-data="(\w+)">', html).group(1)
        scripts = re.findall(r'<script defer src="/js/([\w-]+\.js)"', html)
        assert scripts[:2] == ["api.js", "layout.js"] and len(scripts) == 3, name
        assert f"Alpine.data('{component}'" in (UI_DIR / "js" / scripts[2]).read_text(), name


@pytest.mark.parametrize("path", ["/css/app.css", "/js/api.js", "/js/login.js", "/vendor/pico.min.css",
                                  "/img/favicon.svg", "/", "/settings.html", "/static/login.html"])
async def test_the_page_and_its_files_are_checked_for_changes_on_every_load(page, path):
    """After an upgrade a browser must not keep using an old stylesheet or script (it did: no cache header, so it
    cached by heuristics): no-cache, i.e. a quick revalidation (ETag / Last-Modified) each time."""
    http, _, _ = page
    async with http.get(path) as r:
        assert r.status == 200
        assert r.headers.get("Cache-Control") == "no-cache", (path, r.headers.get("Cache-Control"))


# ----------------------------------------------------------------------------- same origin only

@pytest.mark.parametrize("headers, status", [
    ({"Origin": "http://evil.example"}, 403),
    ({"Origin": "null"}, 403),
    ({"Sec-Fetch-Site": "cross-site"}, 403),
    ({}, 200),                                                  # curl, scripts
])
async def test_changes_from_another_site_are_refused(page, headers, status):
    http, _, _ = page
    async with http.post("/api/sources/sp/stop", json={}, headers=headers) as r:
        assert r.status == status


async def test_changes_from_the_connectors_own_pages_pass(page):
    http, _, _ = page
    origin = str(http._base_url).rstrip("/")
    async with http.post("/api/sources/sp/stop", json={}, headers={"Origin": origin, "Sec-Fetch-Site": "same-origin"}) as r:
        assert r.status == 200
    async with http.get("/api/status", headers={"Origin": "http://evil.example"}) as r:
        assert r.status == 200 and "Access-Control-Allow-Origin" not in r.headers     # reading: no CORS


# ----------------------------------------------------------------------------- behind a reverse proxy

PUBLIC = "https://connector.plant.example.com"


@pytest.mark.parametrize("origin, host, headers, allowed, ok", [
    ("http://127.0.0.1:8001", "127.0.0.1:8001", {}, (), True),                       # no proxy
    (PUBLIC, "127.0.0.1:8001", {"X-Forwarded-Host": "connector.plant.example.com"}, (), True),
    (PUBLIC, "127.0.0.1:8001", {"X-Forwarded-Host": "connector.plant.example.com, edge.lb"}, (), True),   # a chain
    (PUBLIC, "127.0.0.1:8001", {"Forwarded": "for=10.0.0.9;proto=https;host=connector.plant.example.com"}, (), True),
    (PUBLIC, "127.0.0.1:8001", {"Forwarded": 'For="[2001:db8::1]";Host="connector.plant.example.com"'}, (), True),
    (PUBLIC, "127.0.0.1:8001", {}, (PUBLIC,), True),                                  # nginx: Host rewritten
    (PUBLIC, "127.0.0.1:8001", {}, ("HTTPS://Connector.Plant.Example.com:443/",), True),    # normalised
    ("https://connector.plant.example.com:8443", "127.0.0.1:8001", {}, ("https://connector.plant.example.com:8443",), True),
    (PUBLIC, "127.0.0.1:8001", {}, (), False),                                        # the 403 the README explains
    ("http://localhost:18088", "localhost", {}, (), False),        # nginx's $host drops the port: $http_host keeps it
    ("http://connector.plant.example.com", "127.0.0.1:8001", {}, (PUBLIC,), False),    # another scheme
    ("https://evil.example", "127.0.0.1:8001", {"X-Forwarded-Host": "connector.plant.example.com"}, (PUBLIC,), False),
    ("null", "127.0.0.1:8001", {}, (PUBLIC,), False),
])
def test_which_origins_count_as_the_connectors_own(origin, host, headers, allowed, ok):
    from sparkplug_connector.web.web_server import allowed_origins, same_origin
    assert same_origin(origin, host, headers, allowed_origins(list(allowed))) is ok


def test_an_allowed_origin_that_isnt_one_is_ignored_and_logged(caplog):
    from sparkplug_connector.web.web_server import allowed_origins
    with caplog.at_level("ERROR"):
        assert allowed_origins([PUBLIC, "connector.plant.example.com", "ftp://x", "https://a.example/path"]) == frozenset(
            {PUBLIC})
    assert caplog.text.count("web_ui.allowed_origins") == 3


@contextlib.asynccontextmanager
async def page_with(tmp_path, monkeypatch, **web_ui):
    """The web server (bridge running, sources that never connect: the ``started`` fixture) on test_web_sources'
    config with these ``web_ui`` settings; yields (http session, port, server)."""
    import socket
    from test_web_sources import CONFIG
    from sparkplug_connector.core import bridge as core_bridge
    from sparkplug_connector.core.config_loader import ConfigLoader
    from sparkplug_connector.web.web_server import WebServer
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("SPTEST_PASSWORD", "s3cret")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    raw = yaml.safe_load(CONFIG.format(port=port))
    raw["web_ui"].update(web_ui)
    path = tmp_path / "connector.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    config = ConfigLoader(config_path=path).load()
    bridge = core_bridge.Bridge(config)
    await bridge.start()
    server = WebServer(config=config, bridge=bridge, config_path=path)
    await server.start()
    try:
        async with aiohttp.ClientSession(f"http://127.0.0.1:{port}") as http:
            yield http, port, server
    finally:
        await server.stop()
        await server.lifecycle.shutdown()


@pytest.fixture
async def proxied(tmp_path, monkeypatch, started):
    """As behind nginx with its default Host rewrite: web_ui.allowed_origins."""
    async with page_with(tmp_path, monkeypatch, allowed_origins=[PUBLIC]) as (http, _, _):
        yield http


async def test_behind_a_proxy_the_public_origin_may_change_things(proxied):
    async with proxied.post("/api/sources/sp/stop", json={}, headers={"Origin": PUBLIC}) as r:
        assert r.status == 200
    async with proxied.post("/api/sources/sp/stop", json={}, headers={"Origin": "https://evil.example"}) as r:
        assert r.status == 403


# ----------------------------------------------------------------------------- settings

async def test_settings_are_saved_to_the_file_for_the_next_start(page):
    http, path, _ = page
    async with http.get("/api/settings") as r:
        body = await r.json()
    names = [s["name"] for s in body["sections"]]
    assert names[0] == "Diagnostics" and body["values"]["diagnostics.enabled"] is None and not body["restartRequired"]
    header = path.read_text().splitlines()[0]
    async with http.put("/api/settings", json={"values": {"diagnostics.enabled": True, "diagnostics.group_id": "SITE",
                                                           "backpressure.memory_queue.max_size": "5000"}}) as r:
        assert r.status == 200
        body = await r.json()
    assert body["restartRequired"] and body["values"]["diagnostics.group_id"] == "SITE"
    raw = yaml.safe_load(path.read_text())
    assert raw["diagnostics"] == {"enabled": True, "group_id": "SITE"} and raw["backpressure"]["memory_queue"]["max_size"] == 5000
    assert path.read_text().splitlines()[0] == header                                   # the file's header kept
    assert raw["sources"][0]["password"] == "${env:SPTEST_PASSWORD}"                    # placeholders kept
    async with http.get("/api/status") as r:
        assert (await r.json())["restart_required"] is True
    async with http.put("/api/settings", json={"values": {"diagnostics.group_id": ""}}) as r:   # back to the default
        assert r.status == 200
    assert "group_id" not in yaml.safe_load(path.read_text())["diagnostics"]


@pytest.mark.parametrize("values, field", [
    ({"backpressure.memory_queue.max_size": "many"}, "backpressure.memory_queue.max_size"),
    ({"backpressure.memory_queue.max_size": "0"}, "backpressure.memory_queue.max_size"),
    ({"backpressure.memory_queue.max_size": "inf"}, "backpressure.memory_queue.max_size"),      # was HTTP 500
    ({"zerobus.batch.linger_seconds": "nan"}, "zerobus.batch.linger_seconds"),                 # was saved as .nan
    ({"zerobus.drain_timeout_seconds": "-inf"}, "zerobus.drain_timeout_seconds"),
    ({"backpressure.memory_queue.drop_policy": "random"}, "backpressure.memory_queue.drop_policy"),
    ({"diagnostics.enabled": "maybe"}, "diagnostics.enabled"),
    ({"zerobus.auth.client_secret": "x"}, "zerobus.auth.client_secret"),        # not a setting of this page
])
async def test_settings_are_checked(page, values, field):
    http, path, _ = page
    before = path.read_text()
    async with http.put("/api/settings", json={"values": values}) as r:
        assert r.status == 400 and field in (await r.json())["errors"]
    assert path.read_text() == before


# ----------------------------------------------------------------------------- certificates

def _upload(name: str, content: bytes) -> aiohttp.FormData:
    form = aiohttp.FormData()
    form.add_field("file", content, filename=name, content_type="application/octet-stream")
    return form


async def test_a_big_upload_is_refused_without_reading_it_whole(page, monkeypatch):
    """The 1 MB limit is checked while reading: a multi-GB upload must not be buffered in memory first."""
    http, _, _ = page
    read = {"bytes": 0}
    chunk = aiohttp.BodyPartReader.read_chunk

    async def counted(self, size=aiohttp.BodyPartReader.chunk_size):
        data = await chunk(self, size)
        read["bytes"] += len(data)
        return data
    monkeypatch.setattr(aiohttp.BodyPartReader, "read_chunk", counted)
    # (the server answers before the body ends and discards the rest: aiohttp's lingering close, up to 10 s
    # unless the client closes the connection)
    async with http.post("/api/certs", data=_upload("huge.pem", b"x" * (5 * 1024 * 1024)),
                         headers={"Connection": "close"}) as r:
        assert r.status == 413
    assert read["bytes"] <= 1024 * 1024 + 2 * aiohttp.BodyPartReader.chunk_size


async def test_certificates_upload_replace_list_delete(page, tmp_path):
    http, _, _ = page
    async with http.post("/api/certs", data=_upload("plant-ca.pem", b"-----BEGIN CERTIFICATE-----\n")) as r:
        assert r.status == 200 and (await r.json())["name"] == "plant-ca.pem"
    store = tmp_path / ".sparkplug_connector" / "certs"                 # HOME is tmp_path
    assert stat.S_IMODE(os.stat(store / "plant-ca.pem").st_mode) == 0o600
    async with http.post("/api/certs", data=_upload("plant-ca.pem", b"new")) as r:
        assert r.status == 409
    async with http.post("/api/certs?overwrite=true", data=_upload("plant-ca.pem", b"new")) as r:
        assert r.status == 200
    assert (store / "plant-ca.pem").read_bytes() == b"new"
    for name, content, status in (("../evil.pem", b"x", 400), ("a.b.c", b"x", 400), (".pem", b"x", 400),
                                  ("empty.pem", b"", 400), ("big.pem", b"x" * (1024 * 1024 + 1), 413)):
        async with http.post("/api/certs", data=_upload(name, content)) as r:
            assert r.status == status, name
    async with http.get("/api/certs") as r:
        body = await r.json()
    assert [f["name"] for f in body["files"]] == ["plant-ca.pem"] and body["maxSize"] == 1024 * 1024
    async with http.delete("/api/certs/plant-ca.pem") as r:
        assert r.status == 200
    async with http.delete("/api/certs/plant-ca.pem") as r:
        assert r.status == 404


def test_tls_file_names_resolve_in_the_store(tmp_path):
    source = {"name": "sp", "security": {"ca_cert_path": "ca.pem", "client_cert_path": "/etc/certs/c.pem",
                                         "client_key_path": "keys/k.pem", "check_hostname": False}}
    resolved = resolve_tls_paths(source, tmp_path)
    assert resolved["security"] == {"ca_cert_path": str(tmp_path / "ca.pem"), "client_cert_path": "/etc/certs/c.pem",
                                    "client_key_path": "keys/k.pem", "check_hostname": False}
    assert source["security"]["ca_cert_path"] == "ca.pem"                       # the config itself unchanged
    assert resolve_tls_paths({"name": "x"}, tmp_path) == {"name": "x"}
    assert used_by([source, {"name": "y"}], "ca.pem") == ["sp"] and used_by([source], "k.pem") == []
    with pytest.raises(ValueError):
        CertStore(tmp_path).path("../x")


# ----------------------------------------------------------------------------- the Sources dialog's fields

async def test_the_dialogs_connection_fields_are_saved_and_the_tls_file_is_resolved(page, started, tmp_path):
    http, path, bridge = page
    edit = {**SP, "username": "plant", "keepalive": 30, "topics": ["spBv1.0/Plant1/#", "spBv1.0/STATE/#"],
            "security": {"ca_cert_path": "plant-ca.pem", "client_cert_path": "", "client_key_path": "",
                         "check_hostname": True}}
    async with http.put("/api/sources/sp", json=edit) as r:
        assert r.status == 200, await r.text()
    s = saved(path)["sp"]
    assert (s["username"], s["keepalive"], s["topics"]) == ("plant", 30, ["spBv1.0/Plant1/#", "spBv1.0/STATE/#"])
    assert s["security"] == {"ca_cert_path": "plant-ca.pem", "check_hostname": True}
    assert s["password"] == "${env:SPTEST_PASSWORD}"                            # not sent: kept
    async with http.post("/api/sources/sp/stop", json={}):
        pass
    async with http.post("/api/sources/sp/start", json={}) as r:
        assert r.status == 200
    name, config = started[-1]
    assert name == "sp" and config["security"]["ca_cert_path"] == str(tmp_path / ".sparkplug_connector" / "certs" / "plant-ca.pem")


@pytest.mark.parametrize("edit, message", [
    ({"topics": ["spBv1.0/#/x"]}, "topic filter"),
    ({"topics": []}, "at least one"),
    ({"keepalive": 0}, "Keep-alive"),
    ({"security": {"client_cert_path": "c.pem"}}, "needs its key"),
    ({"security": {"insecure": True}}, "unknown insecure"),
])
async def test_the_dialogs_connection_fields_are_checked(page, edit, message):
    http, path, _ = page
    before = path.read_text()
    async with http.put("/api/sources/sp", json={**SP, **edit}) as r:
        assert r.status == 400 and message in (await r.json())["message"]
    assert path.read_text() == before


@pytest.mark.parametrize("password", ["typed-secret", "${credential:sources.sp.password}", "${env:BAD-NAME}",
                                      "${env:X:default}"])
async def test_a_password_comes_only_from_an_environment_variable(page, password):
    http, path, _ = page
    before = path.read_text()
    async with http.put("/api/sources/sp", json={**SP, "password": password}) as r:
        assert r.status == 400 and "environment variables" in (await r.json())["message"]
    assert path.read_text() == before


async def test_the_page_sees_the_passwords_variable_and_whether_it_is_set(page, monkeypatch):
    http, _, _ = page
    async with http.get("/api/sources") as r:
        sp = next(s for s in (await r.json())["sources"] if s["name"] == "sp")
    assert (sp["password_env"], sp["password_env_set"], sp["password_other"]) == ("SPTEST_PASSWORD", True, False)
    assert "s3cret" not in str(sp)
    monkeypatch.delenv("SPTEST_PASSWORD")
    async with http.get("/api/sources") as r:
        sp = next(s for s in (await r.json())["sources"] if s["name"] == "sp")
    assert (sp["password_env"], sp["password_env_set"]) == ("SPTEST_PASSWORD", False)


async def test_removing_the_variable_removes_the_password(page):
    http, path, bridge = page
    async with http.put("/api/sources/sp", json={**SP, "password": ""}) as r:
        assert r.status == 200
    assert "password" not in saved(path)["sp"] and not running(bridge, "sp").get("password")


async def test_a_typed_env_placeholder_stays_one_in_the_file(page, monkeypatch):
    http, path, bridge = page
    monkeypatch.setenv("PLANT_MQTT_PASSWORD", "from-env")
    async with http.put("/api/sources/sp", json={**SP, "password": "${env:PLANT_MQTT_PASSWORD}"}) as r:
        assert r.status == 200
    assert saved(path)["sp"]["password"] == "${env:PLANT_MQTT_PASSWORD}"
    assert running(bridge, "sp")["password"] == "from-env"


# ----------------------------------------------------------------------------- read-only

async def test_a_read_only_page_shows_everything_and_changes_nothing(tmp_path, monkeypatch, started):
    """web_ui.read_only: the status for operators, nothing they can change (with the login off, or any role)."""
    async with page_with(tmp_path, monkeypatch, read_only=True) as (http, _, server):
        for path in ("/api/status", "/api/sources", "/api/settings", "/api/certs"):
            async with http.get(path) as r:
                assert r.status == 200, path
        for method, path in (("POST", "/api/bridge/restart"), ("POST", "/api/sources/sp/stop"),
                             ("PUT", "/api/settings"), ("DELETE", "/api/sources/sp"), ("POST", "/api/zerobus/stop")):
            async with http.request(method, path, json={"values": {}}) as r:
                body = await r.json()
                assert r.status == 403 and "read-only" in body["message"], (method, path, r.status, body)
        assert server.lifecycle.state == "running" and "sp" in server.bridge.clients     # nothing stopped
        async with http.get("/api/auth/status") as r:
            assert (await r.json())["read_only"] is True


async def test_the_page_is_writable_by_default(page):
    http, _, _ = page
    async with http.get("/api/auth/status") as r:
        assert (await r.json())["read_only"] is False


# ----------------------------------------------------------------------------- save conflicts

async def test_a_settings_save_on_an_old_version_is_refused_and_changes_nothing(page):
    """Two people on the page: the second save, made on what the first one's save replaced, must not overwrite it."""
    http, path, _ = page
    async with http.get("/api/settings") as r:
        old = r.headers["X-Config-Version"]
    async with http.put("/api/settings", json={"values": {"diagnostics.group_id": "FIRST"}},
                        headers={"If-Match": old}) as r:
        assert r.status == 200
        new = r.headers["X-Config-Version"]
    assert new != old
    before = path.read_text()
    async with http.put("/api/settings", json={"values": {"diagnostics.group_id": "SECOND"}},
                        headers={"If-Match": old}) as r:
        body = await r.json()
        assert r.status == 412 and "changed since" in body["message"]
    assert path.read_text() == before
    async with http.put("/api/settings", json={"values": {"diagnostics.group_id": "SCRIPT"}}) as r:
        assert r.status == 200                                                  # no If-Match: as before


async def test_source_and_zerobus_saves_on_an_old_version_are_refused_before_anything_changes(page):
    from test_web_sources import EDIT
    http, path, bridge = page
    async with http.get("/api/sources") as r:
        old = r.headers["X-Config-Version"]
    async with http.put("/api/settings", json={"values": {"diagnostics.group_id": "OTHER"}}) as r:
        assert r.status == 200                                                  # someone else's save
    before, running = path.read_text(), sorted(bridge.clients)
    sp = next(s for s in bridge.sources if s["name"] == "sp")
    stale = {"If-Match": old}
    for method, url, body in (("PUT", "/api/sources/sp", EDIT), ("DELETE", "/api/sources/sp", None),
                              ("POST", "/api/sources", {**EDIT, "name": "new"}),
                              ("POST", "/api/zerobus/config", {"workspace_host": "https://x.cloud.databricks.com"})):
        async with http.request(method, url, json=body, headers=stale) as r:
            assert r.status == 412, (method, url, r.status, await r.text())
    assert path.read_text() == before and sorted(bridge.clients) == running
    assert next(s for s in bridge.sources if s["name"] == "sp") == sp


# ----------------------------------------------------------------------------- second review (2026-10-29)

async def test_two_saves_on_one_version_cannot_both_pass(page, monkeypatch):
    """R2-5: the version was checked before the handler's awaits (a source edit restarts the source: seconds), so a
    second save on the same version passed while the first one was still running, and overwrote it."""
    import asyncio
    from test_web_sources import EDIT
    http, path, bridge = page
    async with http.get("/api/sources") as r:
        version = {"If-Match": r.headers["X-Config-Version"]}
    update = bridge.update_source

    async def slow_update(name, config):
        await asyncio.sleep(0.3)
        await update(name, config)
    monkeypatch.setattr(bridge, "update_source", slow_update)

    async def send(method, url, body):
        async with http.request(method, url, json=body, headers=version) as r:
            return r.status
    first = asyncio.create_task(send("PUT", "/api/sources/sp", {**EDIT, "offline_after_seconds": 41}))
    await asyncio.sleep(0.1)                                    # the source edit is restarting the source
    second = await send("PUT", "/api/settings", {"values": {"diagnostics.group_id": "SECOND"}})
    assert (await first, second) == (200, 412)
    assert saved(path)["sp"]["offline_after_seconds"] == 41 and "SECOND" not in path.read_text()
