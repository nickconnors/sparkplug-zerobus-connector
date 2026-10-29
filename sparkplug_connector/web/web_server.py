"""The connector's web server: REST API and web page.

- Sparkplug B sources: add, edit, remove, start, stop
- ZeroBus target configuration and diagnostics
- Status, metrics and the Sparkplug health; the edge nodes and rebirth requests
- Settings (the connector-wide ones) and the uploaded certificates
- Optional login (OAuth2) with role-based permissions

The web page (``static/ui/``) is plain HTML, Alpine.js (CSP build) and Pico CSS, served as files: no build step.
Same origin only: a request that changes something from a page of another origin is refused (no CORS).
Secrets (a source's MQTT password, the Zerobus client secret) come from environment variables only: the page sets
``${env:NAME}`` and never sees or sends a secret's value.
"""

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

from urllib.parse import urlsplit

from aiohttp import web
from typing import Callable

from sparkplug_connector.core.cert_store import CertError, CertStore, MAX_SIZE, certs_dir, used_by
from sparkplug_connector.core.lifecycle import BridgeLifecycle
from sparkplug_connector.web import settings_schema

# Authentication and authorization (off by default)
from sparkplug_connector.web.auth import AuthenticationManager, auth_middleware
from sparkplug_connector.web.rbac import (
    require_permission, require_role, Permission, Role, get_role_info
)
from sparkplug_connector.web.security_headers import (
    security_headers_middleware, log_security_headers_status
)
from sparkplug_connector.web.input_validation import validate_source_config, validate_source_name, ValidationError
from sparkplug_connector.web.correlation_middleware import correlation_id_middleware
from sparkplug_connector.core.structured_logging import get_structured_logger
from sparkplug_connector.core.tls_manager import create_ssl_context_from_config

logger = logging.getLogger(__name__)


SECRET_KEY_PARTS = ('password', 'secret', 'token', 'private_key', 'api_key')
ENV_REF = re.compile(r'^\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}$')


def env_secret(value: Any, label: str) -> Optional[str]:
    """A secret as the page may set it: ``${env:NAME}`` (its value comes from the connector's environment), or ''
    (none). ValidationError for anything else: a secret's value is never sent to the connector or written to its file."""
    if value is None or value == '':
        return value
    if isinstance(value, str) and ENV_REF.match(value.strip()):
        return value.strip()
    raise ValidationError(f"{label}: secrets come from environment variables: give the variable's name "
                          "(saved as ${env:NAME}), and set it in the connector's environment")


def env_reference(value: Any) -> Dict[str, Any]:
    """How a secret is configured (for the page): the environment variable it reads and whether that is set, or
    that the file holds something else (a value, or a ${credential:...} placeholder)."""
    if value in (None, ''):
        return {'env': None, 'set': False, 'other': False}
    m = ENV_REF.match(str(value).strip())
    if m:
        return {'env': m.group(1), 'set': bool(os.environ.get(m.group(1))), 'other': False}
    return {'env': None, 'set': True, 'other': True}

UI_DIR = Path(__file__).parent / "static" / "ui"
UI_PAGES = ("index", "sources", "edge-nodes", "zerobus", "settings", "certs")
UI_ASSETS = ("css", "js", "vendor", "img")
SAFE_METHODS = ("GET", "HEAD", "OPTIONS")


@web.middleware
async def same_origin_middleware(request: web.Request, handler):
    """A request that changes something must come from the connector's own pages: one a browser sends from a page of
    another origin (``Origin`` another host, or ``Sec-Fetch-Site: cross-site``) is refused. Clients that send
    neither (curl, scripts) pass; the login, when on, still applies to them."""
    if request.method not in SAFE_METHODS:
        origin = request.headers.get('Origin')
        hosts = {request.host, request.headers.get('X-Forwarded-Host', '')} - {''}
        cross = request.headers.get('Sec-Fetch-Site') == 'cross-site'
        if origin is not None and (origin == 'null' or urlsplit(origin).netloc not in hosts):
            cross = True
        if cross:
            return web.json_response({'status': 'error', 'message': 'Refused: a request from another site'}, status=403)
    return await handler(request)


def _masked(value: Any) -> Any:
    """Sources for the API: values of password/secret/token keys replaced (the forms never send them back)."""
    if isinstance(value, dict):
        return {k: ('********' if v and any(p in str(k).lower() for p in SECRET_KEY_PARTS) else _masked(v))
                for k, v in value.items()}
    if isinstance(value, list):
        return [_masked(v) for v in value]
    return value


def _same(a: Any, b: Any) -> bool:
    """A form value equal to the running one (the file may spell a number or switch as text)."""
    return a == b or (not isinstance(a, (dict, list)) and str(a).strip().lower() == str(b).strip().lower())


def _edited(entry: Dict[str, Any], current: Dict[str, Any], form: Dict[str, Any]) -> Dict[str, Any]:
    """A source's ``entry`` (the running config, or the file's, with its placeholders) after a form edit:
    fields the form doesn't send are kept; a sent field equal to the running value (``current``) keeps
    the entry's spelling; a changed one is set, an empty one removed; nested settings the same way."""
    out = dict(entry)
    for key, value in form.items():
        now = current.get(key)
        if isinstance(value, dict):
            out[key] = _edited(entry.get(key) if isinstance(entry.get(key), dict) else {},
                               now if isinstance(now, dict) else {}, value)
        elif key in entry and _same(value, now):
            continue
        elif value in ('', None):
            out.pop(key, None)
        else:
            out[key] = value
    return out


class WebServer:
    """Web server for configuration and monitoring."""

    def __init__(self, config: Dict[str, Any], bridge, config_path: Optional[Path] = None,
                 lifecycle: Optional[BridgeLifecycle] = None):
        """Initialize web server.

        Args:
            config: Application configuration
            bridge: Bridge instance
            config_path: the file the connector was started with (source changes are saved there;
                None: the package's default config.yaml)
            lifecycle: the bridge's stop/start/restart (None: one on ``bridge`` and ``config_path``)
        """
        self.config = config
        self.bridge = bridge
        self.config_path = Path(config_path) if config_path else None
        self.lifecycle = lifecycle or BridgeLifecycle(bridge, load_config=lambda: self._config_loader().load())
        self.lifecycle.add_listener(self._new_bridge)

        web_ui_config = config.get('web_ui', {})
        self.host = web_ui_config.get('host', '0.0.0.0')
        self.port = web_ui_config.get('port', 8080)
        self.tls_config = web_ui_config.get('tls', {})

        self.app = None
        self.runner = None
        self.auth_manager = None
        self.security_logger = get_structured_logger()
        self.ssl_context = None
        self.restart_required = False       # settings saved that apply at the next start

    async def start(self):
        """Start web server."""
        self.app = web.Application(client_max_size=MAX_SIZE + 64 * 1024)    # a certificate upload and its multipart

        static_dir = Path(__file__).parent / "static"

        # Correlation IDs in the logs
        self.app.middlewares.append(correlation_id_middleware)
        logger.info("✓ Correlation ID tracking enabled")

        # Security headers (logged after the TLS setup); same origin only for changes
        self.app.middlewares.append(security_headers_middleware)
        self.app.middlewares.append(same_origin_middleware)

        # Login (web_ui.authentication)
        auth_config = self.config.get('web_ui', {}).get('authentication', {})
        if auth_config.get('enabled', False):
            self.auth_manager = AuthenticationManager(auth_config)
            await self.auth_manager.setup(self.app)
            # Add auth middleware
            self.app.middlewares.append(auth_middleware)
            logger.info("✓ Authentication enabled")
        else:
            logger.warning("⚠️  Authentication disabled: anyone who reaches the web UI can change the configuration")

        # API Routes
        routes = [
            # Authentication
            web.get('/login', self.handle_login),
            web.get('/login/callback', self.handle_oauth_callback),
            web.post('/logout', self.handle_logout),
            web.get('/api/auth/status', self.get_auth_status),
            web.get('/api/auth/permissions', self.get_user_permissions),
            web.get('/api/auth/roles', self.get_role_info),

            web.get('/api/sources', self.get_sources),
            web.post('/api/sources', self.add_source),
            web.delete('/api/sources/{name}', self.remove_source),
            web.put('/api/sources/{name}', self.update_source),

            web.post('/api/sources/{name}/start', self.start_source),
            web.post('/api/sources/{name}/stop', self.stop_source),
            web.post('/api/sources/{name}/rebirth', self.request_rebirth),

            # Bridge control
            web.post('/api/bridge/start', self.start_bridge),
            web.post('/api/bridge/stop', self.stop_bridge),
            web.post('/api/bridge/restart', self.restart_bridge),

            # ZeroBus
            web.get('/api/zerobus/config', self.get_zerobus_config),
            web.post('/api/zerobus/config', self.update_zerobus_config),
            web.post('/api/zerobus/start', self.start_zerobus),
            web.post('/api/zerobus/stop', self.stop_zerobus),
            web.get('/api/zerobus/diagnostics', self.get_zerobus_diagnostics),

            # Monitoring
            web.get('/api/metrics', self.get_metrics),
            web.get('/api/sparkplug/health', self.get_sparkplug_health),
            web.get('/api/sparkplug/edge-nodes', self.get_sparkplug_edge_nodes),
            web.get('/api/status', self.get_status),

            # Settings and certificates
            web.get('/api/settings', self.get_settings),
            web.put('/api/settings', self.update_settings),
            web.get('/api/certs', self.get_certs),
            web.post('/api/certs', self.upload_cert),
            web.delete('/api/certs/{name}', self.delete_cert),

            # Health
            web.get('/health', self.health_check),

            # Web UI: the pages, then their files
            web.get('/', self.serve_page),
            web.get('/{page}.html', self.serve_page),
        ]

        for route in routes:
            self.app.router.add_route(route.method, route.path, route.handler)

        for folder in UI_ASSETS:
            if (UI_DIR / folder).is_dir():
                self.app.router.add_static(f'/{folder}/', path=str(UI_DIR / folder))
        # the login page
        if static_dir.exists():
            self.app.router.add_static('/static/', path=str(static_dir), name='static')

        # HTTPS (web_ui.tls)
        if self.tls_config.get('enabled', False):
            try:
                self.ssl_context = create_ssl_context_from_config(self.tls_config)
                # Store HSTS config in app for security headers middleware
                self.app.hsts_config = self.tls_config.get('hsts', {})
                logger.info("✓ TLS/SSL enabled")
            except Exception as e:
                logger.error(f"Failed to setup TLS/SSL: {e}")
                logger.warning("⚠️  Falling back to HTTP")
                self.ssl_context = None
        else:
            logger.info("TLS/SSL disabled: the web UI is plain HTTP")

        # Log security headers status
        log_security_headers_status(https_enabled=self.ssl_context is not None)

        # Start server
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port, ssl_context=self.ssl_context)
        await site.start()

        protocol = "https" if self.ssl_context else "http"
        logger.info(f"Web UI started on {protocol}://{self.host}:{self.port}")

    async def stop(self):
        """Stop web server."""
        if self.runner:
            await self.runner.cleanup()
        logger.info("Web UI stopped")

    # API Handlers

    @require_permission(Permission.READ)
    async def get_sources(self, request: web.Request) -> web.Response:
        """Get configured data sources (passwords and secrets masked)."""
        sources = self.bridge.sources if self.bridge is not None else self.config.get('sources', [])
        try:
            in_file = {s.get('name'): s for s in (self._config_loader().load_raw().get('sources') or [])}
        except Exception:  # noqa: BLE001 - no file (tests, a config passed in): nothing to show about the variables
            in_file = {}
        out = []
        for source in _masked(sources):
            ref = env_reference((in_file.get(source.get('name')) or {}).get('password'))
            out.append({**source, 'password_env': ref['env'], 'password_env_set': ref['set'],
                        'password_other': ref['other']})
        return web.json_response({'sources': out})

    def _save_sources(self, change) -> None:
        """Applies ``change`` (sources -> sources) to the sources in the file the connector was started
        with, as written there: ``${env:...}``/``${credential:...}`` placeholders stay placeholders."""
        from sparkplug_connector.core.config_loader import ConfigLoader
        config_loader = ConfigLoader(config_path=self.config_path)
        config = config_loader.load_raw()
        config['sources'] = change(list(config.get('sources') or []))
        if not config_loader.save(config):
            raise RuntimeError(f"saving the configuration to {config_loader.config_path} failed")

    def _source_password(self, name: str, data: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """A source's form data for the running bridge and for the file: the password only as ``${env:NAME}`` (kept so
        in the file, the variable's value for the bridge) or '' (removed). ValidationError for a password's value."""
        if 'password' not in data:
            return data, data
        password = env_secret(data.get('password'), "Password")
        if not password:
            return {**data, 'password': password}, {**data, 'password': password}
        return {**data, 'password': os.environ.get(ENV_REF.match(password).group(1), '')}, {**data, 'password': password}

    @require_permission(Permission.WRITE)
    async def add_source(self, request: web.Request) -> web.Response:
        """Add new data source."""
        try:
            data = await request.json()

            # Validate and sanitize input
            try:
                data = validate_source_config(data)
            except ValidationError as e:
                # Log validation failure with structured logging
                user = request.get('user')
                self.security_logger.validation_failed(
                    user=user.email if user else 'anonymous',
                    action='add_source',
                    reason=str(e),
                    input_data=str(data),
                    component='web_server',
                    source_ip=request.remote
                )
                return web.json_response(
                    {'status': 'error', 'message': f'Validation error: {str(e)}'},
                    status=400
                )

            try:
                running, saved = self._source_password(data['name'], data)
            except ValidationError as e:
                return web.json_response({'status': 'error', 'message': str(e)}, status=400)

            # Add to bridge
            await self.bridge.add_source(
                source_name=data['name'],
                source_type=data['protocol'],
                source_config=running
            )

            # Update config file
            self._save_sources(lambda sources: sources + [saved])

            return web.json_response({'status': 'ok', 'message': f"Source '{data['name']}' added"})

        except Exception as e:
            logger.error(f"Failed to add source: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.DELETE)
    async def remove_source(self, request: web.Request) -> web.Response:
        """Remove data source."""
        try:
            source_name = request.match_info['name']

            # Remove from bridge
            await self.bridge.remove_source(source_name)

            # Update config file
            self._save_sources(lambda sources: [s for s in sources if s.get('name') != source_name])

            return web.json_response({'status': 'ok', 'message': f"Source '{source_name}' removed"})

        except Exception as e:
            logger.error(f"Failed to remove source: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.WRITE)
    async def update_source(self, request: web.Request) -> web.Response:
        """Update data source configuration."""
        try:
            source_name = request.match_info['name']

            # Validate source name from URL
            try:
                source_name = validate_source_name(source_name)
            except ValidationError as e:
                return web.json_response(
                    {'status': 'error', 'message': f'Invalid source name: {str(e)}'},
                    status=400
                )

            data = await request.json()

            # Validate and sanitize input
            try:
                data = validate_source_config(data)
            except ValidationError as e:
                return web.json_response(
                    {'status': 'error', 'message': f'Validation error: {str(e)}'},
                    status=400
                )

            # The form shows only some fields: the others (credentials, topics, TLS, ...) are kept
            current = next((s for s in self.bridge.sources if s.get('name') == source_name), None)
            if current is None:
                return web.json_response({'status': 'error', 'message': f"Source not found: {source_name}"}, status=404)
            try:
                running, saved = self._source_password(source_name, data)
            except ValidationError as e:
                return web.json_response({'status': 'error', 'message': str(e)}, status=400)
            await self.bridge.update_source(source_name, _edited(current, current, running))

            # Update config file (unchanged fields keep the file's spelling, e.g. a ${env:...} placeholder)
            self._save_sources(lambda sources: [
                _edited(s, current, saved) if s.get('name') == source_name else s for s in sources])

            return web.json_response({'status': 'ok', 'message': f"Source '{source_name}' updated"})

        except Exception as e:
            logger.error(f"Failed to update source: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)



    @require_permission(Permission.START_STOP)
    async def start_source(self, request: web.Request) -> web.Response:
        """Start a single configured source."""
        try:
            name = request.match_info['name']
            res = await self.bridge.start_source(name)
            return web.json_response(res, status=200 if res.get('status') == 'ok' else 500)
        except Exception as e:
            logger.error(f"Failed to start source: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.START_STOP)
    async def stop_source(self, request: web.Request) -> web.Response:
        """Stop a single active source."""
        try:
            name = request.match_info['name']
            res = await self.bridge.stop_source(name)
            return web.json_response(res, status=200 if res.get('status') == 'ok' else 500)
        except Exception as e:
            logger.error(f"Failed to stop source: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)


    @require_permission(Permission.START_STOP)
    async def request_rebirth(self, request: web.Request) -> web.Response:
        """Ask an edge node for a rebirth (``{"group_id": ..., "edge_node_id": ...}``): the source sends an NCMD."""
        try:
            name = request.match_info['name']
            try:
                body = await request.json()
            except ValueError:
                body = None
            if not isinstance(body, dict):
                return web.json_response({'status': 'error', 'message': 'a JSON object with group_id and edge_node_id'},
                                         status=400)
            res = await self.bridge.request_rebirth(name, body.get('group_id'), body.get('edge_node_id'))
            code = res.pop('code', None)
            status = 200 if res['status'] == 'ok' else {'not_found': 404, 'invalid': 400}.get(code, 409)
            return web.json_response(res, status=status)
        except Exception as e:
            logger.error(f"Failed to request a rebirth: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.READ)
    async def get_zerobus_config(self, request: web.Request) -> web.Response:
        """Get ZeroBus configuration."""
        zerobus_config = self.config.get('zerobus', {})

        # Don't expose sensitive credentials: the secret's environment variable, and whether it is set
        safe_config = {**zerobus_config}
        if 'auth' in safe_config:
            safe_config['auth'] = {
                'client_id': safe_config['auth'].get('client_id', ''),
                'client_secret': '***' if safe_config['auth'].get('client_secret') else ''
            }
        try:
            raw_auth = (self._config_loader().load_raw().get('zerobus') or {}).get('auth') or {}
        except Exception:  # noqa: BLE001 - no file
            raw_auth = {}
        ref = env_reference(raw_auth.get('client_secret'))
        safe_config.setdefault('auth', {}).update({'client_secret_env': ref['env'], 'client_secret_env_set': ref['set'],
                                                   'client_secret_other': ref['other']})
        return web.json_response(safe_config)


    @require_permission(Permission.READ)
    async def get_zerobus_diagnostics(self, request: web.Request) -> web.Response:
        """Return ZeroBus config diagnostics (no node metrics)."""
        import os
        import socket
        from urllib.parse import urlparse
        from sparkplug_connector.core.credential_manager import CredentialManager

        deep = request.query.get('deep', '').lower() in ('1', 'true', 'yes')

        cfg = self.config.get('zerobus', {})
        default_target = cfg.get('default_target', {}) or {}
        auth = cfg.get('auth', {}) or {}

        workspace_host = cfg.get('workspace_host') or default_target.get('workspace_host')
        zerobus_endpoint = cfg.get('zerobus_endpoint')

        table = {
            'catalog': default_target.get('catalog'),
            'schema': default_target.get('schema'),
            'table': default_target.get('table'),
        }

        def _has_value(v: object) -> bool:
            return isinstance(v, str) and bool(v.strip())

        def _mask(v: object) -> str:
            if not _has_value(v):
                return ""
            s = str(v).strip()
            if len(s) <= 10:
                return "***"
            return f"{s[:4]}…{s[-4:]}"

        def _cred_present(key: str) -> bool:
            # Best-effort presence check for encrypted credential store.
            try:
                cm = CredentialManager()
                val = cm.get_credential(key)
                return _has_value(val)
            except Exception:
                return False

        env_client_id = os.getenv("CONNECTOR_ZEROBUS_CLIENT_ID", "").strip()
        env_client_secret = os.getenv("CONNECTOR_ZEROBUS_CLIENT_SECRET", "").strip()
        client_id_val = auth.get("client_id", "")
        client_secret_val = auth.get("client_secret", "")

        client_id_source = (
            "env"
            if env_client_id
            else "credential_store"
            if _cred_present("zerobus.client_id")
            else "config"
        )
        client_secret_source = (
            "env"
            if env_client_secret
            else "credential_store"
            if _cred_present("zerobus.client_secret")
            else "config"
        )

        result: dict[str, object] = {
            'enabled': bool(cfg.get('enabled', False)),
            'workspace_host': workspace_host or '',
            'zerobus_endpoint': zerobus_endpoint or '',
            'target_table': table,
            'auth': {
                'client_id_present': _has_value(client_id_val),
                'client_id_source': client_id_source,
                'client_id_masked': _mask(client_id_val),
                'client_secret_present': _has_value(client_secret_val),
                'client_secret_source': client_secret_source,
            },
            'master_password_set': bool(os.getenv('CONNECTOR_MASTER_PASSWORD')),
            'checks': {},
        }

        async def _dns_check(host: str) -> dict[str, object]:
            loop = asyncio.get_running_loop()
            try:
                addrs = await asyncio.wait_for(
                    loop.getaddrinfo(host, 443, type=socket.SOCK_STREAM),
                    timeout=1.5,
                )
                ips = sorted({a[4][0] for a in addrs})[:6]
                return {'ok': True, 'addresses': ips}
            except Exception as e:
                return {'ok': False, 'error': str(e)}

        async def _tcp_check(host: str) -> dict[str, object]:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, 443),
                    timeout=2.0,
                )
                writer.close()
                await writer.wait_closed()
                return {'ok': True}
            except Exception as e:
                return {'ok': False, 'error': str(e)}

        checks: dict[str, object] = {}

        if _has_value(zerobus_endpoint):
            # the endpoint may be written with or without https:// (the SDK takes both)
            zerobus_host = (urlparse(zerobus_endpoint).netloc or zerobus_endpoint).split(':', 1)[0]
            dns, tcp = await asyncio.gather(
                _dns_check(zerobus_host),
                _tcp_check(zerobus_host),
            )
            checks['zerobus_dns_ok'] = dns['ok']
            if dns.get('addresses'):
                checks['zerobus_dns_addresses'] = dns['addresses']
            if dns.get('error'):
                checks['zerobus_dns_error'] = dns['error']

            checks['zerobus_tcp_443_ok'] = tcp['ok']
            if tcp.get('error'):
                checks['zerobus_tcp_443_error'] = tcp['error']
        else:
            checks['zerobus_dns_ok'] = False
            checks['zerobus_tcp_443_ok'] = False

        if _has_value(workspace_host):
            try:
                host = urlparse(workspace_host).netloc or workspace_host
                if ':' in host:
                    host = host.split(':', 1)[0]

                dns, tcp = await asyncio.gather(
                    _dns_check(host),
                    _tcp_check(host),
                )

                checks['workspace_dns_ok'] = dns['ok']
                if dns.get('addresses'):
                    checks['workspace_dns_addresses'] = dns['addresses']
                if dns.get('error'):
                    checks['workspace_dns_error'] = dns['error']

                checks['workspace_tcp_443_ok'] = tcp['ok']
                if tcp.get('error'):
                    checks['workspace_tcp_443_error'] = tcp['error']
            except Exception as e:
                checks['workspace_parse_ok'] = False
                checks['workspace_parse_error'] = str(e)
        else:
            checks['workspace_dns_ok'] = False
            checks['workspace_tcp_443_ok'] = False

        if deep:
            try:
                from sparkplug_connector.core.confirmed_writer import ZerobusRowStreams

                streams = ZerobusRowStreams(zerobus_endpoint, workspace_host,
                                            f"{table['catalog']}.{table['schema']}.{table['table']}",
                                            auth.get('client_id', ''), auth.get('client_secret', ''))
                stream = await asyncio.wait_for(streams.open(), timeout=12.0)
                await stream.close()
                checks['deep_stream_create_ok'] = True
            except Exception as e:
                checks['deep_stream_create_ok'] = False
                checks['deep_stream_create_error'] = str(e)

        result['checks'] = checks
        return web.json_response(result)



    @require_permission(Permission.CONFIGURE)
    async def update_zerobus_config(self, request: web.Request) -> web.Response:
        """Update ZeroBus configuration."""
        try:
            data = await request.json()
            if not isinstance(data, dict):
                return web.json_response({'status': 'error', 'message': 'A JSON object is needed'}, status=400)

            auth_in = data.get('auth') if isinstance(data.get('auth'), dict) else {}
            warning: str | None = None
            client_id = str(auth_in.get('client_id') or '').strip()
            # the secret only as ${env:NAME}: its value is never sent here or written to the file
            try:
                client_secret = env_secret(str(auth_in.get('client_secret') or '').strip(), "Client secret")
            except ValidationError as e:
                return web.json_response({'status': 'error', 'message': str(e)}, status=400)
            if client_secret and not os.environ.get(ENV_REF.match(client_secret).group(1)):
                warning = (f"{client_secret} is not set in the connector's environment: Zerobus can't sign in until it "
                           "is (then restart the connector)")

            # Update config file (IMPORTANT: load raw to avoid persisting injected secrets): the file the
            # connector was started with, as written there (${env:...} placeholders stay placeholders)
            from sparkplug_connector.core.config_loader import ConfigLoader
            config_loader = ConfigLoader(config_path=self.config_path)
            config_raw = config_loader.load_raw()

            existing = config_raw.get('zerobus', {}) or {}
            merged = {**existing, **data}

            # Sanitize auth before persisting
            existing_auth = (existing.get('auth') or {}) if isinstance(existing, dict) else {}
            auth_out = dict(existing_auth) if isinstance(existing_auth, dict) else {}

            if client_id:
                auth_out['client_id'] = client_id
            if client_secret:
                auth_out['client_secret'] = client_secret

            merged['auth'] = auth_out
            config_raw['zerobus'] = merged

            ok = config_loader.save(config_raw)
            if not ok:
                return web.json_response({'status': 'error', 'message': 'Failed to save configuration'}, status=500)

            # Reload injected config into memory so GET/diagnostics reflect latest saved values
            self.config = config_loader.load(inject_credentials=True)
            try:
                # Update bridge config view (without restarting running tasks)
                self.bridge.config = self.config
                self.bridge.sources = self.config.get('sources', [])
                self.bridge.zerobus_config = self.config.get('zerobus', {}) or {}
                self.bridge.zerobus_config_enabled = bool(self.bridge.zerobus_config.get('enabled', False))
            except Exception:
                pass

            resp = {'status': 'ok', 'message': 'ZeroBus configuration updated'}
            if warning:
                resp['warning'] = warning
            return web.json_response(resp)

        except Exception as e:
            logger.error(f"Failed to update ZeroBus config: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)








    def _new_bridge(self, bridge, config: Dict[str, Any]) -> None:
        """A start after a stop built a new bridge from the file: it has every saved setting."""
        self.bridge, self.config = bridge, config
        self.restart_required = False

    def _lifecycle_busy(self) -> Optional[web.Response]:
        if self.lifecycle.busy:
            return web.json_response({'status': 'error', 'message': f'The bridge is {self.lifecycle.state}: wait for it',
                                      'state': self.lifecycle.state}, status=409)
        return None

    @require_permission(Permission.START_STOP)
    async def start_bridge(self, request: web.Request) -> web.Response:
        """The bridge and its enabled sources, in the background (after a stop: a new bridge from the file)."""
        busy = self._lifecycle_busy()
        if busy is not None:                # (a Response is a mapping: an empty one is falsy)
            return busy
        self.lifecycle.submit('start')
        return web.json_response({'status': 'ok', 'message': 'Bridge starting', 'state': self.lifecycle.state})

    @require_permission(Permission.START_STOP)
    async def stop_bridge(self, request: web.Request) -> web.Response:
        """Every source stopped and the queued rows written (up to zerobus.drain_timeout_seconds); the page stays."""
        busy = self._lifecycle_busy()
        if busy is not None:
            return busy
        try:
            await self.lifecycle.stop()
            return web.json_response({'status': 'ok', 'message': 'Bridge stopped', 'state': self.lifecycle.state})
        except Exception as e:
            logger.error(f"Failed to stop bridge: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.START_STOP)
    async def restart_bridge(self, request: web.Request) -> web.Response:
        """Stop, then a new bridge from the file (the saved settings apply), in the background."""
        busy = self._lifecycle_busy()
        if busy is not None:
            return busy
        self.lifecycle.submit('restart')
        return web.json_response({'status': 'ok', 'message': 'Bridge restarting', 'state': self.lifecycle.state})


    @require_permission(Permission.START_STOP)
    async def start_zerobus(self, request: web.Request) -> web.Response:
        """Start ZeroBus streaming."""
        try:
            result = await self.bridge.start_zerobus()
            return web.json_response(result)
        except Exception as e:
            logger.error(f"Failed to start ZeroBus: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.START_STOP)
    async def stop_zerobus(self, request: web.Request) -> web.Response:
        """Stop ZeroBus streaming."""
        try:
            result = await self.bridge.stop_zerobus()
            return web.json_response(result)
        except Exception as e:
            logger.error(f"Failed to stop ZeroBus: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.READ)
    async def get_metrics(self, request: web.Request) -> web.Response:
        """Get connector metrics."""
        try:
            metrics = self.bridge.get_metrics()
            return web.json_response(metrics)
        except Exception as e:
            logger.error(f"Failed to get metrics: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.READ)
    async def get_sparkplug_health(self, request: web.Request) -> web.Response:
        """Per Sparkplug source: host state, MQTT, rows, Zerobus writer, NBIRTH calls and write-ahead log."""
        try:
            return web.json_response(self.bridge.sparkplug_health())
        except Exception as e:
            logger.error(f"Failed to get Sparkplug health: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.READ)
    async def get_sparkplug_edge_nodes(self, request: web.Request) -> web.Response:
        """Per Sparkplug source: its edge nodes and devices, online or offline, since when."""
        try:
            return web.json_response(self.bridge.sparkplug_edge_nodes())
        except Exception as e:
            logger.error(f"Failed to get the Sparkplug edge nodes: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.READ)
    async def get_status(self, request: web.Request) -> web.Response:
        """Get connector status."""
        try:
            status = self.bridge.get_status()
            status['restart_required'] = self.restart_required      # settings saved that apply at the next start
            status['bridge_state'] = self.lifecycle.state             # starting, running, stopping or stopped
            status['sources'] = [{'name': src.get('name'), 'enabled': bool(src.get('enabled', True)),
                                  'running': src.get('name') in self.bridge.clients} for src in self.bridge.sources]
            return web.json_response(status)
        except Exception as e:
            logger.error(f"Failed to get status: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    async def health_check(self, request: web.Request) -> web.Response:
        """Health check endpoint."""
        return web.json_response({'status': 'healthy'})

    async def _auth_gate(self, request: web.Request) -> Optional[web.Response]:
        """Return a redirect response if auth is enabled and user not authenticated."""
        if self.auth_manager:
            user = request.get('user')
            if not user:
                return web.HTTPFound('/static/login.html')
        return None

    async def serve_page(self, request: web.Request) -> web.StreamResponse:
        """A page of the web UI (``/`` is ``index.html``); behind the login when it is on."""
        page = request.match_info.get('page', 'index')
        if page not in UI_PAGES:
            raise web.HTTPNotFound()
        gated = await self._auth_gate(request)
        if gated:
            return gated
        return web.FileResponse(path=UI_DIR / f"{page}.html", headers={'Cache-Control': 'no-cache'})

    # Settings and certificates

    def _config_loader(self):
        from sparkplug_connector.core.config_loader import ConfigLoader
        return ConfigLoader(config_path=self.config_path)

    def _settings_response(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        loader = self._config_loader()
        return {**settings_schema.schema(), 'values': settings_schema.values(raw), 'file': str(loader.config_path),
                'restartRequired': self.restart_required}

    @require_permission(Permission.READ)
    async def get_settings(self, request: web.Request) -> web.Response:
        """The connector-wide settings (settings_schema): their schema and the file's values."""
        return web.json_response(self._settings_response(self._config_loader().load_raw()))

    @require_permission(Permission.CONFIGURE)
    async def update_settings(self, request: web.Request) -> web.Response:
        """``{"values": {key: value}}``: saved to the file (blank: back to the default); applied at the next start."""
        try:
            data = await request.json()
        except Exception:  # noqa: BLE001 - not JSON
            return web.json_response({'status': 'error', 'message': 'A JSON body is needed'}, status=400)
        changes = data.get('values') if isinstance(data, dict) else None
        if not isinstance(changes, dict):
            return web.json_response({'status': 'error', 'message': 'values: an object of settings'}, status=400)
        loader = self._config_loader()
        raw, errors = settings_schema.apply(loader.load_raw(), changes)
        if errors:
            return web.json_response({'status': 'error', 'message': 'Some settings are not valid', 'errors': errors},
                                     status=400)
        if not loader.save(raw):
            return web.json_response({'status': 'error', 'message': f'Saving {loader.config_path} failed'}, status=500)
        if changes:
            self.restart_required = True
        return web.json_response(self._settings_response(raw))

    def _cert_store(self) -> CertStore:
        return CertStore(certs_dir(self.config))

    @require_permission(Permission.READ)
    async def get_certs(self, request: web.Request) -> web.Response:
        store = self._cert_store()
        sources = self.bridge.sources if self.bridge is not None else self.config.get('sources', [])
        files = [{'name': f.name, 'size': f.size, 'modified': f.modified, 'usedBy': used_by(sources, f.name)}
                 for f in store.list()]
        return web.json_response({'files': files, 'maxSize': MAX_SIZE, 'directory': str(store.dir)})

    @require_permission(Permission.CONFIGURE)
    async def upload_cert(self, request: web.Request) -> web.Response:
        """A multipart upload (field ``file``; ``overwrite=true`` replaces a file of that name)."""
        try:
            reader = await request.multipart()
        except Exception:  # noqa: BLE001 - not multipart
            return web.json_response({'status': 'error', 'message': 'A multipart upload is needed'}, status=400)
        overwrite = request.query.get('overwrite') == 'true'
        async for part in reader:
            if part.name != 'file':
                continue
            content = await part.read(decode=False)
            try:
                saved = self._cert_store().save(part.filename or '', bytes(content), overwrite)
            except CertError as e:
                return web.json_response({'status': 'error', 'message': str(e)}, status=e.status)
            return web.json_response({'status': 'ok', 'name': saved.name, 'size': saved.size, 'modified': saved.modified})
        return web.json_response({'status': 'error', 'message': 'No file in the upload'}, status=400)

    @require_permission(Permission.DELETE)
    async def delete_cert(self, request: web.Request) -> web.Response:
        name = request.match_info['name']
        if not self._cert_store().delete(name):
            return web.json_response({'status': 'error', 'message': f'No file named {name}'}, status=404)
        return web.json_response({'status': 'ok', 'message': f'Deleted {name}'})

    # Authentication Handlers

    async def handle_login(self, request: web.Request) -> web.Response:
        """Redirect to OAuth provider for authentication."""
        if not self.auth_manager:
            return web.json_response(
                {'status': 'error', 'message': 'Authentication not enabled'},
                status=503
            )
        return await self.auth_manager.handle_login(request)

    async def handle_oauth_callback(self, request: web.Request) -> web.Response:
        """Handle OAuth callback after user authentication."""
        if not self.auth_manager:
            return web.json_response(
                {'status': 'error', 'message': 'Authentication not enabled'},
                status=503
            )
        return await self.auth_manager.handle_oauth_callback(request)

    async def handle_logout(self, request: web.Request) -> web.Response:
        """Logout user and clear session."""
        if not self.auth_manager:
            return web.json_response(
                {'status': 'error', 'message': 'Authentication not enabled'},
                status=503
            )
        return await self.auth_manager.handle_logout(request)

    async def get_auth_status(self, request: web.Request) -> web.Response:
        """Get current authentication status and user info."""
        auth_manager = request.app.get('auth_manager')
        auth_enabled = auth_manager and auth_manager.enabled if auth_manager else False

        user = request.get('user')
        if not user:
            return web.json_response({
                'auth_enabled': auth_enabled,
                'authenticated': False,
                'user': None
            })

        return web.json_response({
            'auth_enabled': auth_enabled,
            'authenticated': True,
            'user': user.to_dict()
        })

    async def get_user_permissions(self, request: web.Request) -> web.Response:
        """Get permissions for current user."""
        user = request.get('user')
        if not user:
            return web.json_response(
                {'status': 'error', 'message': 'Not authenticated'},
                status=401
            )

        return web.json_response({
            'role': user.role.value,
            'permissions': user.get_permissions()
        })

    async def get_role_info(self, request: web.Request) -> web.Response:
        """Get information about all roles and permissions."""
        return web.json_response(get_role_info())
