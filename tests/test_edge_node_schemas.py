"""BirthNotifier (the NBIRTH -> procedure call, create_edge_node_schema by default), nbirth_procedures
and DatabricksSql (mocked HTTP)."""
from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from sparkplug_connector.sparkplug.edge_node_schemas import (
    MAX_ATTEMPTS,
    Birth,
    BirthNotifier,
    DatabricksSql,
    SqlError,
    birth_models,
    nbirth_procedures,
    node_schema_name,
    retry_delay,
)

RAW = "cat.stage_db.sparkplug_raw"
V1 = "v1.stage_db.create_edge_node_schema"
SDP = "sdp.stage_db.create_edge_node_schema"


class FakeSql:
    """Records the statements; readable after ``readable_after`` checks; CALLs fail ``fail_calls`` times. A CALL
    that succeeds makes the edge node's schema in the procedure's catalog and the views of ``models`` (none for the
    next ``calls_making_nothing`` calls); ``lookups``: the information_schema queries (catalog, schema)."""

    def __init__(self, models=()):
        self.log: list[str] = []
        self.lookups: list[tuple[str, str]] = []
        self.readable_after = self.fail_calls = self.checks = self.call_attempts = self.calls_making_nothing = 0
        self.models = tuple(models)
        self.objects: dict[tuple[str, str], set[str]] = {}      # (catalog, schema) -> views

    async def execute(self, statement, params):
        if "information_schema" in statement:
            catalog = statement.split(" FROM ")[1].split(".information_schema")[0]
            self.lookups.append((catalog, params["schema"]))
            views = self.objects.get((catalog, params["schema"]))
            if ".views " in statement:
                return [[v] for v in sorted(views or ())]
            return [["0" if views is None else "1"]]
        if statement.startswith("SELECT count(*)"):
            self.log.append(f"check {params['msg_id']}")
            self.checks += 1
            return [["1" if self.checks > self.readable_after else "0"]]
        self.log.append(statement.replace(":prefix, :group_id, :edge_node_id",
                                          f"{params['prefix']}, {params['group_id']}, {params['edge_node_id']}"))
        self.call_attempts += 1
        if self.call_attempts <= self.fail_calls:
            raise RuntimeError("PERMISSION_DENIED (test)")
        if self.calls_making_nothing:
            self.calls_making_nothing -= 1
            return []
        catalog = statement.split("CALL ")[1].split(".")[0]
        views = self.objects.setdefault((catalog, node_schema_name(params["prefix"], params["group_id"],
                                                                   params["edge_node_id"])), set())
        views.update(v for m in self.models for v in (f"{m}_vw", f"{m}_asof_vw", m))
        return []

    def calls(self):
        return [s for s in self.log if s.startswith("CALL")]


async def no_sleep(_):
    await asyncio.sleep(0)


def row(message_type, group, edge, msg_id, msg="{}"):
    return json.dumps({"msg_id": msg_id, "msg_topic": f"spBv1.0/{group}/{message_type}/{edge}", "namespace": "spBv1.0",
                       "group_id": group, "message_type": message_type, "edge_node_id": edge, "device_id": None,
                       "msg": msg, "inserted_at": "1"}, separators=(",", ":"))


def nbirth_msg(*templates: str) -> str:
    """An NBIRTH's Tahu JSON with bdSeq, a plain metric and a Template definition per name."""
    metrics = [{"name": "bdSeq", "timestamp": 1, "dataType": "Int64", "value": 0},
               {"name": "Node Control/Rebirth", "timestamp": 1, "dataType": "Boolean", "value": False}]
    metrics += [{"name": t, "timestamp": 1, "dataType": "Template",
                 "value": {"version": "", "reference": "", "isDefinition": True, "metrics": []}} for t in templates]
    return json.dumps({"timestamp": 1, "metrics": metrics, "seq": 0}, separators=(",", ":"))


async def wait_until(condition, timeout=5.0):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), timeout)


async def test_an_nbirth_is_checked_readable_then_every_procedure_is_called():
    sql = FakeSql()
    sql.readable_after = 2                                  # not readable on the first two checks
    n = BirthNotifier(sql, RAW, [V1, SDP], coalesce_s=0, sleep=no_sleep)
    n.accept([row("DDATA", "G", "E", "d1"), row("NBIRTH", "G", "E", "n1"), row("DBIRTH", "G", "E", "b1")])
    await wait_until(lambda: len(sql.calls()) == 2)
    assert sql.log == ["check n1", "check n1", "check n1",
                       "CALL v1.stage_db.create_edge_node_schema(nb, G, E, false)",
                       "CALL sdp.stage_db.create_edge_node_schema(nb, G, E, false)"]
    s = n.status()
    assert (s.births, s.calls, s.failures, s.gave_up, s.pending, s.last_error) == (1, 2, 0, 0, 0, None)
    await n.stop()


async def test_other_message_types_call_nothing():
    sql = FakeSql()
    n = BirthNotifier(sql, RAW, [V1], coalesce_s=0, sleep=no_sleep)
    n.accept([row("DBIRTH", "G", "E", "b"), row("NDEATH", "G", "E", "d"), row("DDATA", "G", "E", "x")])
    await asyncio.sleep(0.1)
    assert sql.log == []
    await n.stop()


async def test_a_burst_of_nbirths_of_one_edge_node_is_one_call_for_the_latest():
    sql = FakeSql()
    n = BirthNotifier(sql, RAW, [V1], coalesce_s=0.3)
    n.accept([row("NBIRTH", "G", "E", "n1"), row("NBIRTH", "G", "E2", "m1")])
    n.accept([row("NBIRTH", "G", "E", "n2")])
    await wait_until(lambda: len(sql.calls()) == 2)
    await asyncio.sleep(0.4)
    assert sorted(sql.calls()) == ["CALL v1.stage_db.create_edge_node_schema(nb, G, E, false)",
                                   "CALL v1.stage_db.create_edge_node_schema(nb, G, E2, false)"]
    assert "check n2" in sql.log and "check n1" not in sql.log
    assert n.status().births == 3
    await n.stop()


async def run_directly(n: BirthNotifier, b: Birth):
    """A birth's attempts on this task, as the scheduler runs them after each retry delay."""
    n.latest_for_test(b)
    done: set[str] = set()
    for attempt in range(MAX_ATTEMPTS):
        if await n.run(b, attempt, done):
            return


async def test_a_failed_call_is_retried_and_procedures_already_done_arent_called_again():
    sql = FakeSql()
    sql.fail_calls = 1                                      # the first procedure's first CALL fails
    n = BirthNotifier(sql, RAW, [V1, SDP], coalesce_s=0, sleep=no_sleep)
    await run_directly(n, Birth("G", "E", "n1"))
    assert sql.calls() == ["CALL v1.stage_db.create_edge_node_schema(nb, G, E, false)",       # failed
                           "CALL v1.stage_db.create_edge_node_schema(nb, G, E, false)",       # retried
                           "CALL sdp.stage_db.create_edge_node_schema(nb, G, E, false)"]
    s = n.status()
    assert (s.failures, s.calls, s.pending) == (1, 2, 0) and "PERMISSION_DENIED" in s.last_error


async def test_gives_up_after_max_attempts():
    sql = FakeSql()
    sql.fail_calls = 10 ** 6
    n = BirthNotifier(sql, RAW, [V1], coalesce_s=0, sleep=no_sleep)
    await run_directly(n, Birth("G", "E", "n1"))
    s = n.status()
    assert (s.failures, s.gave_up, s.pending) == (MAX_ATTEMPTS, 1, 0)


def test_retry_delays_back_off_to_two_minutes():
    assert [retry_delay(a) for a in (0, 1, 4, 5)] == [5, 10, 80, 120]


def test_the_raw_table_must_be_a_three_part_name():
    for name in ("sparkplug_raw; DROP TABLE x", "s.sparkplug_raw", "c..t", "c.s.t`; DROP TABLE x; --", "c.s.t\nx"):
        with pytest.raises(ValueError):
            BirthNotifier(FakeSql(), name, [V1])


async def test_the_raw_table_is_quoted_so_any_unity_catalog_name_works():
    statements = []

    class Sql:
        async def execute(self, statement, params):
            statements.append(statement)
            return [["1"]]

    n = BirthNotifier(Sql(), "my-catalog.stage_db.sparkplug_raw", [V1], coalesce_s=0, sleep=no_sleep)
    n.accept([row("NBIRTH", "G", "E", "n1")])
    await wait_until(lambda: len(statements) >= 2)
    assert statements[0] == "SELECT count(*) FROM `my-catalog`.`stage_db`.`sparkplug_raw` WHERE msg_id = :msg_id"
    await n.stop()


async def test_a_procedure_of_your_own_is_called_with_the_same_arguments():
    sql = FakeSql()
    n = BirthNotifier(sql, RAW, ["ops.hooks.on_nbirth", "`my-cat`.stage_db.`build views`"], coalesce_s=0, sleep=no_sleep)
    n.accept([row("NBIRTH", "G", "E", "n1")])
    await wait_until(lambda: len(sql.calls()) == 2)
    assert sql.calls() == ["CALL ops.hooks.on_nbirth(nb, G, E, false)",
                           "CALL `my-cat`.stage_db.`build views`(nb, G, E, false)"]
    await n.stop()


def test_nbirth_procedures_from_the_settings():
    assert nbirth_procedures({"procedures": ["a.b.p", " c.d.q "]}) == ["a.b.p", "c.d.q"]
    assert nbirth_procedures({"procedures": "a.b.p, c.d.q,"}) == ["a.b.p", "c.d.q"]     # text, as from an env var
    assert nbirth_procedures({}) == [] and nbirth_procedures({"procedures": None}) == []


def test_node_schemas_is_refused_with_the_new_name():
    with pytest.raises(ValueError, match="node_schemas is now procedures"):
        nbirth_procedures({"node_schemas": ["a.b"]})
    with pytest.raises(ValueError, match="node_schemas is now procedures"):
        nbirth_procedures({"procedures": ["a.b.p"], "node_schemas": ["c.d"]})


@pytest.mark.parametrize("name", [
    "create_edge_node_schema",                          # not three parts
    "stage_db.create_edge_node_schema",
    "a.b.c.d",
    "a..p",
    "a.b.p(1); DROP TABLE x; --",                       # the name goes into the statement's text
    "a.b.p --",
    "a.b.`p` `q`",
    "a.b.`p\nq`",
    "a.b.`p``q`",
])
def test_a_procedure_name_that_isnt_catalog_schema_procedure_is_refused(name):
    with pytest.raises(ValueError, match="catalog.schema.procedure"):
        nbirth_procedures({"procedures": [name]})
    with pytest.raises(ValueError):
        BirthNotifier(FakeSql(), RAW, [name])


# ----------------------------------------------------------------------------- DatabricksSql

def transport(handler):
    calls = []

    def record(request: httpx.Request):
        calls.append(request)
        return handler(request, len(calls))
    return httpx.MockTransport(record), calls


async def test_runs_a_statement_with_parameters_and_polls_until_done():
    def handler(request, n):
        if request.url.path == "/oidc/v1/token":
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})
        if request.method == "POST":
            body = json.loads(request.content)
            assert body["warehouse_id"] == "wh" and body["parameters"] == [{"name": "msg_id", "value": "m1", "type": "STRING"}]
            assert request.headers["Authorization"] == "Bearer tok"
            return httpx.Response(200, json={"statement_id": "s1", "status": {"state": "RUNNING"}})
        return httpx.Response(200, json={"statement_id": "s1", "status": {"state": "SUCCEEDED"}, "result": {"data_array": [["1"]]}})
    t, calls = transport(handler)
    sql = DatabricksSql("https://ws.example.com/", "id", "secret", "wh", transport=t)
    sql.POLL_S = 0
    assert await sql.execute("SELECT count(*) FROM t WHERE msg_id = :msg_id", {"msg_id": "m1"}) == [["1"]]
    assert [c.url.path for c in calls] == ["/oidc/v1/token", "/api/2.0/sql/statements", "/api/2.0/sql/statements/s1"]
    await sql.close()


async def test_the_token_is_reused():
    def handler(request, n):
        if request.url.path == "/oidc/v1/token":
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})
        return httpx.Response(200, json={"statement_id": "s", "status": {"state": "SUCCEEDED"}, "result": {}})
    t, calls = transport(handler)
    sql = DatabricksSql("https://ws.example.com", "id", "secret", "wh", transport=t)
    await sql.execute("SELECT 1", {})
    await sql.execute("SELECT 1", {})
    assert [c.url.path for c in calls].count("/oidc/v1/token") == 1
    await sql.close()


async def test_a_failed_statement_raises_the_warehouses_message():
    def handler(request, n):
        if request.url.path == "/oidc/v1/token":
            return httpx.Response(200, json={"access_token": "tok"})
        return httpx.Response(200, json={"statement_id": "s", "status": {"state": "FAILED", "error": {"message": "PERMISSION_DENIED: no"}}})
    t, _ = transport(handler)
    sql = DatabricksSql("https://ws.example.com", "id", "secret", "wh", transport=t)
    with pytest.raises(SqlError, match="FAILED: PERMISSION_DENIED"):
        await sql.execute("CALL x()", {})
    await sql.close()


async def test_an_http_error_raises_with_the_status_and_body():
    t, _ = transport(lambda request, n: httpx.Response(403, text="Invalid client"))
    sql = DatabricksSql("https://ws.example.com", "id", "secret", "wh", transport=t)
    with pytest.raises(SqlError, match="HTTP 403 on /oidc/v1/token: Invalid client"):
        await sql.execute("SELECT 1", {})
    await sql.close()


# ----------------------------------------------------------------------------- the schema and views after the call

def test_names_as_the_procedure_makes_them():
    assert node_schema_name("nb", "Plant 1", "Line-A.2") == "nb_plant_1_line_a_2"
    assert node_schema_name("nb", "G", "E") == "nb_g_e"
    assert birth_models(nbirth_msg("Motor", "Pump UDT", "_types_/Conveyor-v2", "Motor")) == (
        "motor", "pump_udt", "_types_conveyor_v2")
    assert birth_models(nbirth_msg()) == ()
    assert birth_models("not json") == () and birth_models("[]") == ()


async def test_an_nbirth_with_models_is_checked_for_its_views_in_the_procedures_catalog():
    sql = FakeSql(models=("motor", "pump_udt"))
    n = BirthNotifier(sql, RAW, [V1, "`node-db`.stage_db.create_edge_node_schema"], coalesce_s=0, sleep=no_sleep)
    n.accept([row("NBIRTH", "Plant 1", "Line-A", "n1", nbirth_msg("Motor", "Pump UDT"))])
    await wait_until(lambda: n.status().checked == 2)
    assert sql.lookups == [("v1", "nb_plant_1_line_a"), ("`node-db`", "nb_plant_1_line_a")]
    s = n.status()
    assert (s.calls, s.checked, s.failures, s.pending, s.last_error) == (2, 2, 0, 0, None)
    await n.stop()


async def test_missing_views_fail_the_attempt_and_the_procedure_is_called_again():
    sql = FakeSql(models=("motor",))
    sql.calls_making_nothing = 1                            # the first CALL returns without making anything
    n = BirthNotifier(sql, RAW, [V1, SDP], coalesce_s=0, sleep=no_sleep)
    await run_directly(n, Birth("G", "E", "n1", ("motor",)))
    assert sql.calls() == ["CALL v1.stage_db.create_edge_node_schema(nb, G, E, false)",       # made nothing
                           "CALL v1.stage_db.create_edge_node_schema(nb, G, E, false)",       # called again
                           "CALL sdp.stage_db.create_edge_node_schema(nb, G, E, false)"]
    s = n.status()
    assert (s.calls, s.checked, s.failures, s.gave_up) == (3, 2, 1, 0)
    assert "v1.nb_g_e lacks motor_vw, motor_asof_vw" in s.last_error


async def test_only_the_views_still_missing_are_named():
    sql = FakeSql(models=("motor",))
    n = BirthNotifier(sql, RAW, [V1], coalesce_s=0, sleep=no_sleep)
    await run_directly(n, Birth("G", "E", "n1", ("motor", "pump")))     # the procedure makes no pump views
    s = n.status()
    assert (s.failures, s.gave_up, s.checked) == (MAX_ATTEMPTS, 1, 0)
    assert s.last_error.endswith("lacks pump_vw, pump_asof_vw")
    assert len(sql.calls()) == MAX_ATTEMPTS


async def test_an_nbirth_without_models_needs_its_schema_only():
    sql = FakeSql()
    sql.calls_making_nothing = 1
    n = BirthNotifier(sql, RAW, [V1], coalesce_s=0, sleep=no_sleep)
    await run_directly(n, Birth("G", "E", "n1"))
    s = n.status()
    assert (s.calls, s.checked, s.failures) == (2, 1, 1) and "lacks the schema" in s.last_error


async def test_the_check_can_be_switched_off():
    sql = FakeSql()
    sql.calls_making_nothing = 10
    n = BirthNotifier(sql, RAW, [V1], coalesce_s=0, sleep=no_sleep, check_views=False)
    await run_directly(n, Birth("G", "E", "n1", ("motor",)))
    s = n.status()
    assert (s.calls, s.checked, s.failures) == (1, 0, 0) and sql.lookups == []


async def test_a_newer_nbirth_is_checked_for_its_own_models():
    sql = FakeSql(models=("motor", "pump"))
    n = BirthNotifier(sql, RAW, [V1], coalesce_s=0.2)
    n.accept([row("NBIRTH", "G", "E", "n1", nbirth_msg("Motor"))])
    n.accept([row("NBIRTH", "G", "E", "n2", nbirth_msg("Motor", "Pump"))])
    await wait_until(lambda: n.status().checked == 1)
    assert n.status().failures == 0 and sql.lookups == [("v1", "nb_g_e")]
    await n.stop()


async def test_a_schema_only_group_needs_its_schema_only():
    """The connector's diagnostics edge nodes: their NBIRTH has models, but only the schema is checked."""
    sql = FakeSql()                                          # the procedure makes the schema, no views
    n = BirthNotifier(sql, RAW, [V1], coalesce_s=0, sleep=no_sleep, schema_only_groups=frozenset({"BRIDGE"}))
    await run_directly(n, Birth("BRIDGE", "Bridge Info", "n1", ("bridge",)))
    await run_directly(n, Birth("G", "E", "n2", ("motor",)))  # another group: its views are checked
    s = n.status()
    assert (s.checked, s.failures) == (1, MAX_ATTEMPTS) and "nb_g_e lacks motor_vw" in s.last_error
