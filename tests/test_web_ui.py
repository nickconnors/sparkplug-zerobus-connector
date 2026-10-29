"""The web page's server side: the pages and their files, the strict CSP, same origin only, the Settings API, the
certificate store and its use by the sources, and the source fields the Sources dialog edits (username, password,
keep-alive, topics, TLS files)."""
from __future__ import annotations

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
