# Sparkplug B → Databricks Zerobus connector

A connector that subscribes to an MQTT server carrying **Sparkplug B** (for example Ignition with MQTT
Transmission publishing to a Chariot MQTT server, or any Sparkplug 3.0 edge) and writes **one
row per Sparkplug message** to a Delta table through [Zerobus Ingest](https://docs.databricks.com/aws/en/ingestion/zerobus-overview).

- **One row per message** in a `sparkplug_raw` table: the topic split into columns, the payload as Eclipse
  Tahu's JSON (byte for byte what Java Tahu produces) in a `VARIANT` column. Splitting it into metrics is
  left to SQL ([docs/UNITY_CATALOG_SCHEMAS.md](docs/UNITY_CATALOG_SCHEMAS.md) has sample queries).
- **Sparkplug primary host application** (optional): ONLINE only while Databricks accepts the rows, so the
  edges buffer (store and forward) whenever they can't get there.
- **Confirmed writes**: a batch leaves the connector only once Zerobus has acknowledged it; an optional
  write-ahead log keeps unacknowledged rows across a crash or restart.
- **A stored procedure call per NBIRTH** (optional), for example to (re)build per-model views.
- **Strict Sparkplug sequencing** (by default): messages put back in `seq` order, an edge node's messages
  dropped after a gap until its rebirth, rebirth requests (NCMD) to edge nodes it hasn't seen born, as Eclipse
  Tahu's host library decides them (ported and tested against it); or a row for every message
  (`sequence_handling: keep_all`).
- **Web UI** to add, edit, start and stop sources and Zerobus, with an optional login; a health endpoint for
  Docker.

Details of every behaviour, setting and counter: [sparkplug_connector/sparkplug/README.md](sparkplug_connector/sparkplug/README.md).

## Primary host or plain subscriber

Sparkplug publishes births and data at MQTT QoS 0, so the MQTT server can't queue them for a subscriber
that is away. Only the edge can buffer, and it does so for its **primary host**. Choose one of two ways
to run the connector:

| | Primary host (`primary_host_id` set) | Plain subscriber (`primary_host_id` empty) |
|---|---|---|
| Edge setting | MQTT Transmission's Primary Host ID = the connector's `primary_host_id`, store and forward on | nothing to change |
| Connector down, or Databricks unreachable | the edges buffer and send the backlog (flagged historical) once it is ONLINE again: nothing lost | messages published meanwhile are lost |
| Start | the edges rebirth when it goes ONLINE | it asks each edge node it finds publishing for a rebirth (NCMD, ~5 s after its first message; the MQTT user needs publish rights on `spBv1.0/+/NCMD/+`), unless `block_auto_rebirth: true` (with `sequence_handling: keep_all`) |
| Other hosts | while the connector is OFFLINE, MQTT Transmission **disconnects from the MQTT server for every subscriber**, not only for the connector | no effect on anything else |

MQTT Transmission has **one** primary host per transmitter. If another application is already its primary
host (another bridge, an Ignition gateway with MQTT Engine), either the connector takes over that role, or
it runs as a plain subscriber next to it. Run exactly one connector per Primary Host ID: two with the same
id fight over STATE, and two with different ids write every row twice.

## Quick start

### 1. Databricks

A table, and a service principal with an OAuth secret that may write it (names are examples; any catalog,
schema and table work):

```sql
CREATE TABLE IF NOT EXISTS bridge_stage_db.stage_db.sparkplug_raw (
  msg_id STRING, msg_topic STRING, namespace STRING, group_id STRING, message_type STRING,
  edge_node_id STRING, device_id STRING, msg VARIANT, inserted_at DECIMAL(38,0)
)
CLUSTER BY (message_type, group_id, edge_node_id, device_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

GRANT USE CATALOG ON CATALOG bridge_stage_db TO `<application id>`;
GRANT USE SCHEMA ON SCHEMA bridge_stage_db.stage_db TO `<application id>`;
GRANT MODIFY, SELECT ON TABLE bridge_stage_db.stage_db.sparkplug_raw TO `<application id>`;
```

The column comments and sample queries are in [docs/UNITY_CATALOG_SCHEMAS.md](docs/UNITY_CATALOG_SCHEMAS.md);
the grants for the NBIRTH call in the [Sparkplug README](sparkplug_connector/sparkplug/README.md#databricks-setup).

### 2. Configure

Copy [sparkplug_connector/config/config.yaml](sparkplug_connector/config/config.yaml) to `my-config.yaml`.
It has one example source, `sparkplug-example`, switched off. In your copy:

- set `zerobus.default_target` to your table;
- set `enabled: true` on the source, and `primary_host_id` if the connector is the edges' primary host;
- keep secrets out of the file: it reads them from the environment (`${env:NAME}` or `${env:NAME:default}`).

The environment variables the example reads, for example in a `connector.env` file:

```
ZEROBUS_ENDPOINT=<workspace-id>.zerobus.<region>.cloud.databricks.com
DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
DATABRICKS_CLIENT_ID=<service principal application id>
DATABRICKS_CLIENT_SECRET=<its OAuth secret>
SPARKPLUG_MQTT_ENDPOINT=mqtt://<mqtt server>:1883
SPARKPLUG_MQTT_USERNAME=<mqtt user>
SPARKPLUG_MQTT_PASSWORD=<its password>
```

The MQTT user needs to subscribe to `spBv1.0/#`, to publish rebirth requests on `spBv1.0/+/NCMD/+` (unless
`block_auto_rebirth: true`; some MQTT servers, Chariot among them, close the connection on a refused publish)
and, as a primary host, to publish its own STATE topic (`spBv1.0/STATE/<primary_host_id>`). For **MQTT over TLS** use `mqtts://<host>:8883`; a private CA, client
certificates and the other TLS settings go under the source's `security:` (commented in the example).

### 3. Run it in Docker

```
docker build -t sparkplug-zerobus --build-arg PIP_INDEX_URL=https://pypi.org/simple .
docker run -d --name sparkplug-zerobus --restart unless-stopped --stop-timeout 30 --env-file connector.env \
  -p 127.0.0.1:8001:8001 \
  --mount type=bind,source="$PWD/my-config.yaml",target=/app/sparkplug_connector/config/config.yaml \
  --mount type=volume,source=connector-wal,target=/app/wal \
  sparkplug-zerobus --config /app/sparkplug_connector/config/config.yaml
```

- Your file is mounted over the shipped example and passed with `--config`; edits made on the web page are
  saved into it, so don't mount it read-only. The container runs as uid 1000: on Linux, make the file
  writable by it (`sudo chown 1000 my-config.yaml`), or the page's saves fail.
- Use `--mount`, not `-v`, for the file: if the file doesn't exist, `-v` creates an empty directory in its
  place (on Windows as well), and the connector fails with `IsADirectoryError`; `--mount` stops with an error.
- The `connector-wal` volume keeps the write-ahead log (`zerobus.wal`, on in the example, path `wal` =
  `/app/wal`) across container restarts.
- `--stop-timeout 30`: on `docker stop` the connector first publishes OFFLINE (a primary host's edges start
  buffering), then writes what is queued, which can take up to ~20 s (`drain_timeout_seconds`, 15). Docker's
  default is 10 s, then SIGKILL; with the write-ahead log the rows not written are sent after the next start,
  without it they are lost. In Compose: `stop_grace_period: 30s`.
- Logs go to the console only (`docker logs`; the image sets `CONNECTOR_LOG_FILES=false`): set a size limit
  in Docker's logging options (e.g. `--log-opt max-size=50m --log-opt max-file=5`). Outside Docker the
  connector also writes rotated log files to `/tmp/sparkplug_connector_logs`.
- The image is Python only: build the web UI first (below) to include it; without it `/` explains how.
- The web UI is on port 8001 (`web_ui.port`). Publish it on 127.0.0.1, or switch the login on (below)
  before publishing it more widely.

**Health check.** The image's `HEALTHCHECK` calls `GET /health` on the web UI's port every 30 s
(`CONNECTOR_HEALTH_PORT`, default 8001; set it if you change `web_ui.port`). It reports whether the
process and its web server answer, so `docker ps` shows `unhealthy` when the connector hangs or its web
server is down. It doesn't say whether rows reach Databricks: `GET /api/sparkplug/health` does, per source
(`state`, `rows_written`, `zerobus_failing_since`, `last_error`, the NBIRTH calls and the write-ahead log).

### Or run it from a checkout

```
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m sparkplug_connector --config my-config.yaml
```

## Web UI

`http://127.0.0.1:8001/`: Overview (sources, Zerobus, each source's state), **Sources** (add, edit, delete,
start, stop; the NBIRTH procedure settings), **Edge nodes** (each running source's edge nodes and devices,
online or offline and since when; a rebirth request for an edge node), **ZeroBus** (endpoint, table, credentials, start and stop,
diagnostics), Status, Metrics, Auth. A save goes to the file the connector was started with (`--config`):
fields the form doesn't show stay as written there, `${env:...}` placeholders included; the API never
returns passwords or secrets. The MQTT username, password, topics and TLS settings are set in the file only.

The UI is a React app in `webui/apps/connector-ui`, built into `sparkplug_connector/web/static/connector-ui/`
(not in git):

```
cd webui && npm ci && npm run build
```

### Login (off by default)

`web_ui.authentication.enabled: true` puts every page and API call (except `/health`) behind a login,
with three roles: `admin` (everything), `operator` (add, edit, start and stop sources and Zerobus; no deletes, no ZeroBus settings), `viewer` (read only).

With `method: oauth2` and `provider: databricks` the login is a Databricks workspace's OAuth:

1. An account admin adds an **app connection** (account console → Settings → App connections): redirect URL
   `http(s)://<connector host>:<port>/login/callback`, scope *All APIs*, with a client secret.
2. Set `oauth.workspace_host` and `oauth.redirect_uri` (the same URL) in the config, the client id and secret in
   `OAUTH_CLIENT_ID` / `OAUTH_CLIENT_SECRET`, and a session key in `SESSION_SECRET_KEY`
   (`python -c "import secrets; print(secrets.token_urlsafe(32))"`).
3. `rbac.role_mappings` maps the user's **workspace groups** to roles (e.g. `admins: admin`); anyone else gets
   `default_role`. The user and the groups come from the workspace's SCIM `Me` with the user's token.

Open the UI on the redirect URL's host name (a login started on another name is moved there first). Logout
ends the connector's session; signing in again is immediate while the browser is still signed in to
Databricks. The `azure`, `okta` and `google` providers need authlib's aiohttp client, which current authlib
releases don't include. `method: client_cert` takes the role from a client certificate (needs `web_ui.tls`
with `require_client_cert: true`).

### HTTPS (off by default)

`web_ui.tls.enabled: true` serves the UI over HTTPS with `cert_file` / `key_file` (and HSTS);
`python scripts/generate_tls_cert.py` makes a self-signed set in `~/.sparkplug_connector/certs/`.

## Credentials

- **Environment** (the way for containers): `${env:NAME}` in the file. The encrypted store below lives in the
  container's home directory, so a secret saved on the web page is gone when the container is recreated
  (and isn't stored at all without `CONNECTOR_MASTER_PASSWORD`).
- **Encrypted store**: `${credential:zerobus.client_secret}` in the file reads `~/.sparkplug_connector/credentials.enc`
  (Fernet, key from `CONNECTOR_MASTER_PASSWORD`); the web page's ZeroBus form stores a new client secret
  there and writes only the placeholder. `CONNECTOR_ZEROBUS_CLIENT_ID` / `CONNECTOR_ZEROBUS_CLIENT_SECRET`
  override the store.
- **Encrypted values in the file**: `python scripts/encrypt_config.py` (same master password).

## Layout

| Path | |
|---|---|
| `sparkplug_connector/sparkplug/` | Sparkplug B: Tahu JSON decoder, raw rows, primary host, NBIRTH calls ([README](sparkplug_connector/sparkplug/README.md), [NOTICE](sparkplug_connector/sparkplug/NOTICE.md)) |
| `sparkplug_connector/protocols/sparkplug_client.py` | the MQTT source |
| `sparkplug_connector/core/` | the bridge (`bridge.py`), queues, confirmed writer, write-ahead log, target tables, config loading, credentials, logging, TLS |
| `sparkplug_connector/web/` | web server, login, roles, input validation, security headers |
| `sparkplug_connector/config/config.yaml` | the example configuration |
| `webui/` | the React UI's source |
| `tests/` | the test suite |
| `scripts/` | `encrypt_config.py`, `generate_tls_cert.py` |

## Tests

```
.venv/bin/pip install -r requirements.txt -r tests/requirements.txt
.venv/bin/python -m pytest tests
```

Some tests need **Java 17** (Eclipse Tahu's Java implementation is the reference for the JSON, through
JPype: `TAHU_JVM=<path to libjvm>`, `TAHU_JAVA_CLASSPATH`) or **Docker** with the `eclipse-mosquitto:2`
image (the primary host against a real MQTT server); they are skipped when those aren't available.
`tests/test_live_zerobus.py` writes to a real table and is skipped unless its environment is set (see the
file). The payload fixtures are synthetic, shaped like MQTT Transmission's (`tests/make_fixtures.py`).
`tests/tck/` runs the Sparkplug TCK (below; Docker and the `hivemq/hivemq-ce:2022.1` image, skipped otherwise;
`-m "not tck"` leaves it out).

## Sparkplug TCK

The connector as **primary host** passes the
[Eclipse Sparkplug TCK](https://github.com/eclipse-sparkplug/sparkplug/tree/master/tck) 3.0.0's Host Application
tests, except where it deliberately does less than a full Sparkplug host application (below). This is not a
certification: no listing was requested. The TCK is a HiveMQ broker extension that watches the host's MQTT traffic,
plays edge nodes and devices, and asks the operator questions; `tests/tck/` runs its six Host Application tests
against the connector over the TCK's MQTT control topics, with HiveMQ CE 2022.1 (the version its user guide names),
the packaged TCK (EPL-2.0, downloaded from download.eclipse.org into `~/.cache/sparkplug-tck` on the first run,
SHA-256 checked, not in this repository) and Mosquitto as the second MQTT server. The connector runs in-process
with Zerobus faked (the TCK looks at MQTT only). Each run writes its results to `tests/tck/results/`; the expected
result of every assertion is in `tests/tck/expected/`.

```
docker pull hivemq/hivemq-ce:2022.1 && docker pull eclipse-mosquitto:2
.venv/bin/python -m pytest tests/tck -v
```

| Test | Assertions passed | Not passed |
|---|---|---|
| Session establishment | 43 | 1 not executed: the MQTT 5 clean start (the connector uses MQTT 3.1.1) |
| Session termination | 14 | 1 not executed: OFFLINE published, then the connection dropped without a DISCONNECT (the connector always disconnects cleanly) |
| Message ordering | 22 | |
| Multiple brokers (a source per MQTT server, one `primary_host_id`) | 8 | |
| Send command | 28 | 11 not executed: the test asks in turn for a node rebirth (sent: **Edge nodes**' Request rebirth), a node metric write, a device rebirth and a device metric write. The connector writes nothing to the edge, so the test ends at the node write and the device commands are never asked for |
| Edge session termination | 12 | 3 failed: the metrics marked STALE at a DDEATH or NDEATH. The connector keeps no metric values: staleness is for the tables downstream. The edge node and device offline questions pass, answered from **Edge nodes** |

The **plain subscriber** mode is outside the TCK: every Sparkplug 3.0 host application publishes STATE, so a
connector without `primary_host_id` is a Sparkplug subscriber, not a Sparkplug 3.0 Host Application.

## Provenance

This connector is derived from [pravinva/unified-ot-zerobus-connector](https://github.com/pravinva/unified-ot-zerobus-connector)
(an OPC-UA, MQTT and Modbus connector with a web UI and the Zerobus write path), through the fork
[nickconnors/unified-ot-zerobus-connector](https://github.com/nickconnors/unified-ot-zerobus-connector),
which added the Sparkplug B source, confirmed writes, the write-ahead log, the primary host and the NBIRTH
calls. This repository starts from that fork's `main` at
[`e269da9`](https://github.com/nickconnors/unified-ot-zerobus-connector/commit/e269da9890cff1d038756d26d1423b9e7de3e58b),
history included, and keeps only the Sparkplug B path: the other protocols, discovery, tag normalization,
numeric compression, the OT simulator and the unused security modules were removed.

## License

The original repository has no license file, so none is added here. The Sparkplug B decoder files in
`sparkplug_connector/sparkplug/` are derived from [Eclipse Tahu](https://github.com/eclipse-tahu/tahu) and stay
under the Eclipse Public License 2.0: see [NOTICE.md](NOTICE.md).
