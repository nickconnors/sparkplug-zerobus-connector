from aiohttp import web
from aiohttp.test_utils import make_mocked_request


async def test_connector_root_serves_react_index_when_present():
    from unified_connector.web.web_server import WebServer

    ws = WebServer(config={}, bridge=None)
    ws.auth_manager = None  # no auth gate

    req = make_mocked_request("GET", "/")
    resp = await ws.serve_index(req)

    # React index.html is built into unified_connector/web/static/connector-ui/index.html
    assert isinstance(resp, web.FileResponse)


async def test_connector_root_explains_how_to_build_the_ui_when_it_is_missing(monkeypatch, tmp_path):
    from unified_connector.web import web_server

    monkeypatch.setattr(web_server, "__file__", str(tmp_path / "web_server.py"))   # no static/connector-ui here
    ws = web_server.WebServer(config={}, bridge=None)
    ws.auth_manager = None

    resp = await ws.serve_index(make_mocked_request("GET", "/"))
    assert isinstance(resp, web.Response) and resp.content_type == "text/html"
    assert "npm run build" in resp.text and "/health" in resp.text
