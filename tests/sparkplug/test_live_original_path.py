"""Live: the original write path (no confirmed writes: ZeroBusClient, JSON) on the Zerobus SDK in requirements.txt.
MQTT messages from an MQTT server (in process) -> the mqtt source -> a bronze table, with OPC-UA and Modbus records
alongside; read back with SQL. The test deletes its rows again: use a table of its own.

Skipped unless set:
    SPARKPLUG_LIVE_ENV, SPARKPLUG_LIVE_PROFILE  as for test_live_zerobus.py
    ZEROBUS_LIVE_BRONZE_TABLE                   catalog.schema.table with the connector's columns
                                                (unified_connector/README.md, Target Table; the service
                                                principal: MODIFY, SELECT)
"""
from __future__ import annotations

import asyncio
import gc
import json
import os
import time
import warnings

import pytest

from test_live_zerobus import PROFILE, _env, _query
from test_other_protocols import bronze, records
from test_sparkplug_source import broker, publish, wait_for  # noqa: F401 (broker: fixture)
from unified_connector.core.unified_bridge import UnifiedBridge

TABLE = os.environ.get("ZEROBUS_LIVE_BRONZE_TABLE")

pytestmark = pytest.mark.skipif(not (os.environ.get("SPARKPLUG_LIVE_ENV") and PROFILE and TABLE),
                                reason="live Zerobus test: set SPARKPLUG_LIVE_ENV, SPARKPLUG_LIVE_PROFILE and "
                                       "ZEROBUS_LIVE_BRONZE_TABLE")

MQTT = [("live/line1/temperature", b"21.5"), ("live/line1/count", b"42"), ("live/line1/state", b'{"value": "running"}')]


async def test_other_protocols_rows_land_in_the_bronze_table(broker):
    env = _env()
    endpoint, port = broker
    source = f"live-{int(time.time())}"                    # this run's rows: found and deleted by source_name
    target = dict(zip(("catalog", "schema", "table"), TABLE.split(".")))
    bridge = UnifiedBridge({
        "sources": [{"name": source, "protocol": "mqtt", "endpoint": endpoint, "topics": ["live/#"], "qos": 0,
                     "client_id": "live-original-path"}],
        "zerobus": {"enabled": True, "zerobus_endpoint": env["ZEROBUS_ENDPOINT"],
                    "auth": {"client_id": env["DATABRICKS_CLIENT_ID"], "client_secret": env["DATABRICKS_CLIENT_SECRET"]},
                    "default_target": {"workspace_host": env["DATABRICKS_HOST"], **target},
                    "batch": {"max_records": 100, "timeout_seconds": 0.5}},
        "normalization": {"enabled": False},
        "backpressure": {"disk_spool": {"enabled": False, "encryption": False}},
    })
    others = [r for r in records(source) if r.protocol_type.value != "mqtt"]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await bridge.start()
        try:
            assert bridge.writers == {}                    # the original path
            client = next(iter(bridge.zerobus_clients.values()))
            await wait_for(lambda: source in bridge.clients and bridge.clients[source]._client is not None, 20)
            await asyncio.sleep(1)                         # subscribed (the mqtt source has no event for it)
            await publish(port, MQTT)
            for r in others:
                bridge._on_record(r, source)
            await wait_for(lambda: client.metrics["records_sent"] == len(MQTT) + len(others), 60)
        finally:
            await bridge.stop()
        gc.collect()
    assert client.metrics["failures"] == 0, client.get_metrics()
    assert [str(w.message) for w in caught if "never awaited" in str(w.message)] == []

    sql = f"SELECT to_json(struct(*)) FROM {TABLE} WHERE source_name = '{source}' ORDER BY event_time, browse_path"
    rows = []
    try:
        for _ in range(30):                                # readable a few seconds after the stream closed
            rows = [json.loads(r[0]) for r in _query(sql, env["DATABRICKS_WAREHOUSE_ID"])]
            if len(rows) >= len(MQTT) + len(others):
                break
            await asyncio.sleep(2)
    finally:                                               # leave the test table as it was
        _query(f"DELETE FROM {TABLE} WHERE source_name = '{source}'", env["DATABRICKS_WAREHOUSE_ID"])
    for r in rows:
        assert isinstance(r.pop("ingest_time"), int)
    want = [b for b in bronze(source) if b["endpoint"] != "mqtt://broker:1883"]
    assert rows[:len(want)] == want                        # event times in 2026-09: before the MQTT messages
    mqtt = rows[len(want):]
    assert all(r.pop("event_time") > want[-1]["event_time"] for r in mqtt)
    assert sorted((r["browse_path"], r["value_type"], r["value"], r.get("value_num"), r["vendor_format"]) for r in mqtt) == [
        ("live/line1/count", "int", "42", 42.0, "generic"),
        ("live/line1/state", "json", "running", None, "generic"),
        ("live/line1/temperature", "float", "21.5", 21.5, "generic")]
    assert {(r["source_name"], r["endpoint"], r["namespace"], r["node_id"], r["status"], r["raw"]) for r in mqtt} == {
        (source, endpoint, 0, "", "Good", "")}
