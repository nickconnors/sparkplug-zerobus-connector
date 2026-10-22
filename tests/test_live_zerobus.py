"""Live: the Transmission-shaped payloads -> MQTT broker (in process) -> sparkplug source -> confirmed writes ->
Zerobus -> a test table; read back with SQL and compared with Java Tahu's JSON. The test deletes its rows
again: use a table of its own, never one other jobs read (its change feed keeps the rows).

Skipped unless set:
    SPARKPLUG_LIVE_ENV      env file with ZEROBUS_ENDPOINT, DATABRICKS_HOST, DATABRICKS_CLIENT_ID,
                            DATABRICKS_CLIENT_SECRET (the service principal) and DATABRICKS_WAREHOUSE_ID
    SPARKPLUG_LIVE_PROFILE  Databricks CLI profile for reading the table back
    SPARKPLUG_LIVE_TABLE    catalog.schema.table with the sparkplug_raw DDL (the service principal: MODIFY, SELECT)
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import time

import pytest

from test_sparkplug_source import SKIPPED, broker, publish, transmission_messages, wait_for  # noqa: F401 (broker: fixture)
from unified_connector.core.unified_bridge import UnifiedBridge
from unified_connector.sparkplug.raw_row import COLUMNS

ENV_FILE = os.environ.get("SPARKPLUG_LIVE_ENV")
PROFILE = os.environ.get("SPARKPLUG_LIVE_PROFILE")
TABLE = os.environ.get("SPARKPLUG_LIVE_TABLE")

pytestmark = pytest.mark.skipif(not (ENV_FILE and PROFILE and TABLE),
                                reason="live Zerobus test: set SPARKPLUG_LIVE_ENV, SPARKPLUG_LIVE_PROFILE and SPARKPLUG_LIVE_TABLE")


def _env() -> dict[str, str]:
    lines = pathlib.Path(ENV_FILE).read_text().splitlines()
    return dict(line.split("=", 1) for line in lines if "=" in line and not line.lstrip().startswith("#"))


def _canonical(value) -> str:
    """JSON text with sorted keys and whole-number floats as integers, to compare by value."""
    def norm(v):
        if isinstance(v, float) and v.is_integer():
            return int(v)
        if isinstance(v, dict):
            return {k: norm(x) for k, x in v.items()}
        if isinstance(v, list):
            return [norm(x) for x in v]
        return v
    return json.dumps(norm(value), sort_keys=True)


def _query(sql: str, warehouse_id: str) -> list[list]:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    w = WorkspaceClient(profile=PROFILE)
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=warehouse_id, wait_timeout="50s")
    assert r.status.state == StatementState.SUCCEEDED, r.status
    return r.result.data_array or []


async def test_rows_land_in_the_table_equal_to_java_tahu(broker):
    env = _env()
    endpoint, port = broker
    target = dict(zip(("catalog", "schema", "table"), TABLE.split(".")))
    bridge = UnifiedBridge({
        "sources": [{"name": "chariot", "protocol": "sparkplug", "endpoint": endpoint, "client_id": "live-test"}],
        "zerobus": {"enabled": True, "zerobus_endpoint": env["ZEROBUS_ENDPOINT"],
                    "auth": {"client_id": env["DATABRICKS_CLIENT_ID"], "client_secret": env["DATABRICKS_CLIENT_SECRET"]},
                    "default_target": {"workspace_host": env["DATABRICKS_HOST"], **target}},
        "backpressure": {},
    })
    messages = transmission_messages()
    started = int(time.time() * 1000)
    await bridge.start()
    try:
        await wait_for(lambda: "chariot" in bridge.clients)
        await asyncio.wait_for(bridge.clients["chariot"].subscribed.wait(), 10)
        await publish(port, [(t, p) for t, p, _ in messages] + [(t, p) for t, p, _ in SKIPPED])
        await wait_for(lambda: bridge.clients["chariot"].counters["rows"] == len(messages), 20)
    finally:
        await bridge.stop()                                          # drains: every row acknowledged first
    status = next(iter(bridge.get_metrics()["zerobus_targets"].values()))
    assert status["written"] == len(messages) and status["batch_in_flight"] == 0, status

    columns = ", ".join(c if c != "msg" else "to_json(msg) AS msg" for c in COLUMNS)
    sql = f"SELECT {columns} FROM {TABLE} WHERE msg_topic NOT LIKE 'probe/%' AND inserted_at >= {started} AND edge_node_id = 'Line A'"
    rows = []
    try:
        for _ in range(30):                                           # readable a few seconds after the acknowledgement
            rows = _query(sql, env["DATABRICKS_WAREHOUSE_ID"])
            if len(rows) >= len(messages):
                break
            await asyncio.sleep(2)
    finally:                                                          # leave the test table as it was
        _query(f"DELETE FROM {TABLE} WHERE inserted_at >= {started} AND edge_node_id = 'Line A' "
               f"AND msg_id IN ({', '.join(repr(r[0]) for r in rows) or 'NULL'})", env["DATABRICKS_WAREHOUSE_ID"])
    assert len(rows) == len(messages)
    got = sorted((r[1], _canonical(json.loads(r[7]))) for r in rows)
    want = sorted((t, _canonical(json.loads(j))) for t, _, j in messages)
    assert got == want                                                # by value: VARIANT sorts keys and writes 2 as 2.0
    assert all(r[2] == "spBv1.0" and r[3] == "Plant1" and int(r[8]) >= started for r in rows)
