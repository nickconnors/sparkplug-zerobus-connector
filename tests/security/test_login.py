"""The web page's Databricks OAuth login: the gate, the OAuth state, the session and the roles (the provider faked)."""
from __future__ import annotations

import socket
from urllib.parse import parse_qs, urlsplit

import aiohttp
import pytest
import yaml

from sparkplug_connector.core.bridge import Bridge
from sparkplug_connector.core.config_loader import ConfigLoader
from sparkplug_connector.web import databricks_oauth
from sparkplug_connector.web.web_server import WebServer


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def server(tmp_path, monkeypatch):
    """The web server with the login on; the provider's token and userinfo calls faked."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("SESSION_SECRET_KEY", "a-test-session-secret-of-some-length")
    port = free_port()

    async def exchange(self, code):
        return {"access_token": f"token-for-{code}"} if code == "good-code" else None

    async def userinfo(self, token):
        return {"email": "someone@example.com", "name": "Someone", "groups": ["OT-Operators"]}

    monkeypatch.setattr(databricks_oauth.DatabricksOAuthClient, "exchange_code_for_token", exchange)
    monkeypatch.setattr(databricks_oauth.DatabricksOAuthClient, "get_user_info", userinfo)
    path = tmp_path / "connector.yaml"
    path.write_text(yaml.safe_dump({
        "web_ui": {"enabled": True, "host": "127.0.0.1", "port": port, "authentication": {
            "enabled": True, "method": "oauth2",
            "oauth": {"provider": "databricks", "client_id": "app", "client_secret": "secret",
                      "workspace_host": "https://example.cloud.databricks.com",
                      "redirect_uri": f"http://127.0.0.1:{port}/login/callback"},
            "rbac": {"enabled": True, "role_mappings": {"OT-Operators": "operator"}, "default_role": "viewer"}}},
        "sources": [], "zerobus": {"enabled": False}}))
    config = ConfigLoader(config_path=path).load()
    bridge = Bridge(config)
    web_server = WebServer(config=config, bridge=bridge, config_path=path)
    await web_server.start()
    jar = aiohttp.CookieJar(unsafe=True)               # cookies for an IP address host
    async with aiohttp.ClientSession(f"http://127.0.0.1:{port}", cookie_jar=jar) as http:
        yield http, port
    await web_server.stop()


async def login(http) -> str:
    """GET /login: the state in the provider's authorize URL."""
    async with http.get("/login", allow_redirects=False) as r:
        assert r.status == 302
        location = r.headers["Location"]
    assert location.startswith("https://example.cloud.databricks.com/oidc/v1/authorize?")
    return parse_qs(urlsplit(location).query)["state"][0]


async def test_pages_redirect_to_the_login_and_everything_else_gets_401(server):
    http, _ = server
    async with http.get("/", headers={"Accept": "text/html"}, allow_redirects=False) as r:
        assert r.status == 302 and r.headers["Location"] == "/login"
    for path, accept in [("/api/status", "application/json"), ("/favicon.ico", "image/avif,image/*")]:
        async with http.get(path, headers={"Accept": accept}, allow_redirects=False) as r:
            assert r.status == 401, path
    async with http.get("/health") as r:
        assert r.status == 200


async def test_a_login_started_on_another_host_name_moves_to_the_redirect_uri_s(server):
    _, port = server
    async with aiohttp.ClientSession() as other:
        async with other.get(f"http://localhost:{port}/login", allow_redirects=False) as r:
            assert r.status == 302 and r.headers["Location"] == f"http://127.0.0.1:{port}/login"


async def test_the_favicon_and_a_second_login_dont_spoil_the_first(server):
    http, _ = server
    first = await login(http)
    async with http.get("/favicon.ico", headers={"Accept": "image/*"}, allow_redirects=False) as r:
        assert r.status == 401                          # was a redirect to /login: a new state replaced the first
    second = await login(http)
    assert first != second
    async with http.get(f"/login/callback?code=good-code&state={first}", allow_redirects=False) as r:
        assert r.status == 302 and r.headers["Location"] == "/", await r.text()
    async with http.get("/api/auth/status") as r:
        status = await r.json()
    assert status["authenticated"] and status["user"]["email"] == "someone@example.com"
    assert status["user"]["role"] == "operator"         # OT-Operators mapped by rbac.role_mappings


async def test_a_state_works_once_and_only_in_the_session_that_started_it(server):
    http, port = server
    state = await login(http)
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as other:
        async with other.get(f"http://127.0.0.1:{port}/login/callback?code=good-code&state={state}",
                             allow_redirects=False) as r:
            assert r.status == 403                      # another browser: no such login in progress
    async with http.get(f"/login/callback?code=good-code&state={state}", allow_redirects=False) as r:
        assert r.status == 302
    async with http.post("/logout", allow_redirects=False) as r:
        assert r.status == 200 and (await r.json())["message"] == "Logged out"   # the web UI's fetch
    async with http.get(f"/login/callback?code=good-code&state={state}", allow_redirects=False) as r:
        assert r.status == 403                          # used


async def test_roles_after_login_and_logout(server):
    http, _ = server
    state = await login(http)
    async with http.get(f"/login/callback?code=good-code&state={state}", allow_redirects=False) as r:
        assert r.status == 302
    async with http.get("/api/sources") as r:
        assert r.status == 200                          # read
    async with http.post("/api/zerobus/config", json={"batch": {"max_records": 10}}) as r:
        assert r.status == 403                          # configure: admin only
    async with http.post("/logout", allow_redirects=False) as r:
        pass
    async with http.get("/api/sources") as r:
        assert r.status == 401


def test_a_scim_me_answer_gives_the_user_and_its_groups():
    me = {"userName": "someone@example.com", "displayName": "Some One",
          "emails": [{"value": "other@example.com", "primary": True}],
          "groups": [{"display": "admins", "value": "1"}, {"display": "OT-Operators", "value": "2"}, {"value": "3"}]}
    assert databricks_oauth.scim_user(me) == {"email": "someone@example.com", "name": "Some One",
                                              "groups": ["admins", "OT-Operators"]}
    assert databricks_oauth.scim_user({"emails": [{"value": "a@example.com"}]}) == {
        "email": "a@example.com", "name": "a@example.com", "groups": []}


async def test_the_user_comes_from_the_workspace_s_scim_me_with_the_access_token():
    from aiohttp import web

    seen = {}

    async def me(request):
        seen["auth"] = request.headers.get("Authorization")
        if seen["auth"] != "Bearer good":
            return web.json_response({"detail": "invalid token"}, status=401)
        return web.json_response({"userName": "someone@example.com", "groups": [{"display": "admins"}]})

    app = web.Application()
    app.router.add_get("/api/2.0/preview/scim/v2/Me", me)
    runner = web.AppRunner(app)
    await runner.setup()
    port = free_port()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    try:
        client = databricks_oauth.DatabricksOAuthClient("app", "secret", f"http://127.0.0.1:{port}",
                                                        "http://localhost/login/callback", ["all-apis"])
        assert await client.get_user_info("good") == {"email": "someone@example.com", "name": "someone@example.com",
                                                       "groups": ["admins"]}
        assert seen["auth"] == "Bearer good"
        assert await client.get_user_info("bad") is None
    finally:
        await runner.cleanup()


async def test_a_form_logout_goes_to_the_login_page_not_to_a_new_sign_in(server):
    http, _ = server
    state = await login(http)
    async with http.get(f"/login/callback?code=good-code&state={state}", allow_redirects=False) as r:
        assert r.status == 302
    async with http.post("/logout", headers={"Accept": "text/html"}, allow_redirects=False) as r:
        assert r.status == 302 and r.headers["Location"] == "/static/login.html"
    async with http.get("/api/auth/status") as r:
        assert r.status == 401
