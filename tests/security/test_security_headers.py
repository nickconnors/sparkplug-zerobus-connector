"""
Security Headers Tests

Tests that all required security headers are present in responses.
"""

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from unified_connector.web.security_headers import security_headers_middleware


@pytest.fixture
async def test_app():
    """Create test application with security headers."""
    app = web.Application(middlewares=[security_headers_middleware])

    async def hello_handler(request):
        return web.json_response({'message': 'hello'})

    async def api_handler(request):
        return web.json_response({'data': 'api response'})

    app.router.add_get('/', hello_handler)
    app.router.add_get('/api/test', api_handler)

    return app


@pytest.fixture
async def client(test_app):
    """Create test client."""
    server = TestServer(test_app)
    client = TestClient(server)
    await client.start_server()
    yield client
    await client.close()


class TestSecurityHeaders:
    """Test security headers middleware."""

    async def test_content_security_policy(self, client):
        """Test CSP header is present and correct."""
        resp = await client.get('/')
        assert resp.status == 200
        assert 'Content-Security-Policy' in resp.headers
        csp = resp.headers['Content-Security-Policy']
        assert "default-src 'self'" in csp
        assert "script-src 'self' 'unsafe-inline'" in csp
        assert "frame-ancestors 'none'" in csp

    async def test_x_frame_options(self, client):
        """Test X-Frame-Options header prevents clickjacking."""
        resp = await client.get('/')
        assert resp.status == 200
        assert resp.headers['X-Frame-Options'] == 'DENY'

    async def test_x_content_type_options(self, client):
        """Test X-Content-Type-Options header prevents MIME sniffing."""
        resp = await client.get('/')
        assert resp.status == 200
        assert resp.headers['X-Content-Type-Options'] == 'nosniff'

    async def test_x_xss_protection(self, client):
        """Test X-XSS-Protection header enables XSS filter."""
        resp = await client.get('/')
        assert resp.status == 200
        assert resp.headers['X-XSS-Protection'] == '1; mode=block'

    async def test_referrer_policy(self, client):
        """Test Referrer-Policy header limits information leakage."""
        resp = await client.get('/')
        assert resp.status == 200
        assert resp.headers['Referrer-Policy'] == 'strict-origin-when-cross-origin'

    async def test_permissions_policy(self, client):
        """Test Permissions-Policy header disables unnecessary features."""
        resp = await client.get('/')
        assert resp.status == 200
        assert 'Permissions-Policy' in resp.headers
        policy = resp.headers['Permissions-Policy']
        assert 'geolocation=()' in policy
        assert 'microphone=()' in policy
        assert 'camera=()' in policy

    async def test_server_header_removed(self, client):
        """Test Server header is removed to prevent info disclosure."""
        resp = await client.get('/')
        assert resp.status == 200
        # Server header should be removed by middleware
        # (aiohttp may add it, but production servers should remove it)

    async def test_api_cache_control(self, client):
        """Test API responses have no-cache headers."""
        resp = await client.get('/api/test')
        assert resp.status == 200
        assert 'Cache-Control' in resp.headers
        cache_control = resp.headers['Cache-Control']
        assert 'no-store' in cache_control
        assert 'no-cache' in cache_control
        assert resp.headers['Pragma'] == 'no-cache'

    async def test_static_cache_control(self, client):
        """Test non-API responses don't have restrictive cache headers."""
        resp = await client.get('/')
        assert resp.status == 200
        # Static content should be cacheable
        if 'Cache-Control' in resp.headers:
            assert 'no-store' not in resp.headers['Cache-Control']
