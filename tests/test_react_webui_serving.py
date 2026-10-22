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


async def test_connector_legacy_route_serves_legacy_html():
    from unified_connector.web.web_server import WebServer

    ws = WebServer(config={}, bridge=None)
    ws.auth_manager = None

    req = make_mocked_request("GET", "/legacy")
    resp = await ws.serve_legacy_index(req)
    assert isinstance(resp, web.Response)
    assert resp.content_type == "text/html"
    assert "/static/app.js" in resp.text
    assert "/static/style.css" in resp.text
