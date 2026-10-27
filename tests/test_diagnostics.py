"""The diagnostics rows (sparkplug/diagnostics.py): the scenario of ``diagnostics_scenario.json`` against the rows a
Java host builds for it (``expected/diagnostics/scenario_rows.json``: Eclipse Tahu's builders, a default Jackson
mapper), and the counters from the rows the sources hand on."""
from __future__ import annotations

import asyncio
import json
import pathlib
import zoneinfo

import pytest

from conftest import EXPECTED
from sparkplug_connector.sparkplug.diagnostics import Diagnostics, Tracker, _Client, _EdgeNode, java_date_string
from sparkplug_connector.sparkplug.raw_row import convert

SCENARIO = json.loads((pathlib.Path(__file__).parent / "diagnostics_scenario.json").read_text())
TZ = zoneinfo.ZoneInfo(SCENARIO["time_zone"])


def _scenario_tracker() -> Tracker:
    t = Tracker()
    b = SCENARIO["bridge"]
    t.mqtt_message_count = b["mqtt_message_count"]
    t.models = {f"m{i}" for i in range(b["model_count"])}
    t.instances = {("g", "e"): {f"i{i}" for i in range(b["instance_count"])}}
    for e in SCENARIO["edge_nodes"]:
        t.edge_nodes[(e["group_id"], e["edge_node_id"])] = _EdgeNode(e["connected"], e["connect_ms"], e["disconnect_ms"],
                                                                     e["births"], e["deaths"], e["messages"])
    for c in SCENARIO["clients"]:
        t.clients[c["server_name"]] = _Client(c["client_id"], c["server_url"], c["connected"], c["connect_ms"] or 0,
                                              c["disconnect_ms"] or 0)
    return t


def _diagnostics(tracker: Tracker) -> Diagnostics:
    s = SCENARIO
    ids = s["edge_node_ids"]
    return Diagnostics(tracker, {"group_id": s["group_id"], "bridge_model": s["bridge_model"],
                                 "bridge_info_edge_node_id": ids["bridge"], "edge_node_info_edge_node_id": ids["edge_node"],
                                 "mqtt_client_info_edge_node_id": ids["mqtt_client"]},
                       version=s["bridge"]["version"], primary_host_id=lambda: s["bridge"]["primary_host_id"], tz=TZ)


def test_the_scenario_matches_the_rows_a_java_host_builds():
    tracker = _scenario_tracker()
    d = _diagnostics(tracker)
    rows = d.start_rows(SCENARIO["start_ms"], SCENARIO["metric_ts"])
    tracker.update_required = True
    rows += d.refresh_rows(SCENARIO["refresh_ms"], SCENARIO["metric_ts"])
    expected = json.loads((EXPECTED / "diagnostics" / "scenario_rows.json").read_text())
    assert [(r.message_type, r.edge_node_id, r.device_id) for r in rows] == [
        (e["message_type"], e["edge_node_id"], e["device_id"]) for e in expected]
    for row, e in zip(rows, expected):
        assert row.msg == e["msg"]
    assert {r.group_id for r in rows} == {"BRIDGE"}
    assert rows[0].msg_topic == "spBv1.0/BRIDGE/NBIRTH/Bridge Info" and rows[1].msg_topic == "spBv1.0/BRIDGE/DBIRTH/Bridge Info/BRIDGE"
    assert rows[-1].msg_topic == "spBv1.0/BRIDGE/DDATA/Edge Node Info/EdgeNode"


def test_nothing_changed_no_rows():
    tracker = _scenario_tracker()
    d = _diagnostics(tracker)
    d.start_rows(SCENARIO["start_ms"])
    assert d.refresh_rows(SCENARIO["refresh_ms"]) == []
    tracker.on_client("sparkplug", False, SCENARIO["refresh_ms"])
    assert [r.message_type for r in d.refresh_rows(SCENARIO["refresh_ms"])] == ["DDATA"] * 3


def test_seq_and_bdseq_count_to_255_then_0():
    tracker = Tracker()
    d = Diagnostics(tracker)
    seqs = []
    for i in range(300):
        tracker.update_required = True
        rows = d.start_rows(1) if i == 0 else d.refresh_rows(1)
        seqs.append(json.loads(rows[0].msg)["seq"])                 # the bridge edge node's
    assert seqs[:3] == [0, 2, 3] and 255 in seqs and seqs[seqs.index(255) + 1] == 0   # NBIRTH 0, DBIRTH 1, DDATA...


def test_java_date_strings():
    assert java_date_string(1790696925611, TZ) == "Tue Sep 29 11:48:45 EDT 2026"
    assert java_date_string(0, TZ) == "Wed Dec 31 19:00:00 EST 1969"
    assert java_date_string(0, zoneinfo.ZoneInfo("UTC")) == "Thu Jan 01 00:00:00 UTC 1970"


def test_names_can_be_set_and_default_to_bridge():
    rows = Diagnostics(Tracker()).start_rows(1)
    assert rows[0].msg_topic == "spBv1.0/BRIDGE/NBIRTH/Bridge Info"
    assert '"name":"BRIDGE Version"' in rows[0].msg
    rows = Diagnostics(Tracker(), {"group_id": "SITE_DIAG", "bridge_model": "CONNECTOR", "bridge_info_edge_node_id": " ",
                                   "unknown": "x"}).start_rows(1)
    assert rows[0].msg_topic == "spBv1.0/SITE_DIAG/NBIRTH/Bridge Info"            # blank: the default
    assert '"name":"CONNECTOR Version"' in rows[0].msg and '"name":"Core/CONNECTOR"' in rows[1].msg


def _rows(*messages):
    out = []
    for topic, payload, at in messages:
        o = convert(topic, payload, at)
        assert o.row is not None
        out.append(o.row)
    return out


def test_the_counters_follow_the_rows_the_sources_hand_on():
    from payloads import metric, payload
    from sparkplug_connector.sparkplug import sparkplug_b_pb2 as pb
    definition = metric("Motor", 19, template_value=pb.Payload.Template(is_definition=True))
    instance = metric("Motor 1", 19, template_value=pb.Payload.Template(is_definition=False, template_ref="Motor"))
    bdseq = metric("bdSeq", 4, long_value=0)
    t = Tracker()
    for row in _rows(("spBv1.0/G/NBIRTH/E", payload(bdseq, definition, timestamp=1000, seq=0), 1005),
                     ("spBv1.0/G/DBIRTH/E/D", payload(instance, timestamp=1100, seq=1), 1105),
                     ("spBv1.0/G/DDATA/E/D", payload(metric("x", 3, int_value=1), seq=2), 1200),
                     ("spBv1.0/G/NDATA/E", payload(seq=3), 1300),
                     ("spBv1.0/G/NDEATH/E", payload(bdseq, timestamp=1400, seq=None), 1405)):
        t.on_row(row)
    node = t.edge_nodes[("G", "E")]
    assert (node.online, node.connect_ms, node.disconnect_ms, node.births, node.deaths, node.messages) == (
        False, 1000, 1400, 1, 1, 5)
    assert (t.mqtt_message_count, t.models, t.instances) == (5, {"Motor"}, {("G", "E", "D"): {"Motor 1"}})


def test_mqtt_clients():
    t = Tracker()
    t.add_client("sp", "databricks-sparkplug", "mqtt://chariot:1883")
    t.on_client("unknown", True, 5)                                 # not a known source: ignored
    t.on_client("sp", True, 10)
    t.on_client("sp", False, 20)
    c = t.clients["sp"]
    assert (c.connected, c.connect_ms, c.disconnect_ms) == (False, 10, 20) and list(t.clients) == ["sp"]
    row = Diagnostics(t, tz=TZ).start_rows(30)[5]                   # the MQTT Client Info DBIRTH
    assert row.message_type == "DBIRTH" and row.device_id == "MQTTClient"
    assert '"name":"ClientID","timestamp":30,"dataType":"String","value":"databricks-sparkplug"' in row.msg
    assert '"name":"ServerName","timestamp":30,"dataType":"String","value":"sp"' in row.msg


@pytest.mark.parametrize("value, expected", [(5, 5), ((1 << 31), -(1 << 31))])
def test_counters_wrap_like_a_java_int(value, expected):
    t = Tracker()
    t.mqtt_message_count = value
    assert f'"name":"MQTTMessageCount","timestamp":1,"dataType":"Int32","value":{expected}' in Diagnostics(t).start_rows(1)[0].msg


# ----------------------------------------------------------------------------- through the bridge

from conftest import FIXTURES  # noqa: E402
from test_edge_node_schemas import FakeSql  # noqa: E402
from test_per_target_queues import wait_for  # noqa: E402
from test_sparkplug_source import broker, publish  # noqa: E402,F401 - broker: a fixture
from test_wal import bridge_config, fake_streams  # noqa: E402,F401 - fake_streams: a fixture
from sparkplug_connector.core import bridge as core_bridge  # noqa: E402
from sparkplug_connector.sparkplug import to_tahu_json  # noqa: E402
from sparkplug_connector.sparkplug.edge_node_schemas import birth_models  # noqa: E402

NBIRTH = (FIXTURES / "transmission" / "nbirth-1.bin").read_bytes()


def _written(streams) -> list[dict]:
    return [json.loads(r) for batch in streams.ingested for r in batch]


async def test_the_bridge_writes_the_diagnostics_and_counts_its_sources(tmp_path, fake_streams, broker, monkeypatch):
    monkeypatch.setattr(core_bridge, "REFRESH_S", 0.2)
    made = []

    class Sql(FakeSql):
        def __init__(self, *args):
            super().__init__(models=birth_models(to_tahu_json(NBIRTH)))
            made.append(self)
    monkeypatch.setattr(core_bridge, "DatabricksSql", Sql)
    endpoint, port = broker
    cfg = bridge_config(tmp_path)
    cfg["sources"] = [{"name": "sp", "protocol": "sparkplug", "endpoint": endpoint, "client_id": "diag-host",
                       "edge_node_schemas": {"enabled": True, "warehouse_id": "w",
                                             "procedures": ["c.s.create_edge_node_schema"]}}]
    cfg["diagnostics"] = {"enabled": True}
    bridge = core_bridge.Bridge(cfg)
    await bridge.start()
    try:
        await wait_for(lambda: "sp" in bridge.clients and bridge.clients["sp"].subscribed.is_set(), 10)
        await publish(port, [("spBv1.0/Plant1/NBIRTH/Line A", NBIRTH)])

        def line_a():
            return [r for r in _written(fake_streams) if r["group_id"] == "BRIDGE" and r["message_type"] == "DDATA"
                    and "Core/EdgeNode-Line A" in r["msg"] and '"name":"BirthCount","timestamp"' in r["msg"]
                    and '"dataType":"Int32","value":1}' in r["msg"]]
        await wait_for(line_a, 10)
        await wait_for(lambda: bridge.birth_notifiers["sp"].status().checked == 4, 10)
    finally:
        await bridge.stop()
    written = _written(fake_streams)
    diag = [r for r in written if r["group_id"] == "BRIDGE"]
    assert [(r["message_type"], r["edge_node_id"]) for r in diag[:6]] == [
        ("NBIRTH", "Bridge Info"), ("DBIRTH", "Bridge Info"), ("NBIRTH", "Edge Node Info"), ("DBIRTH", "Edge Node Info"),
        ("NBIRTH", "MQTT Client Info"), ("DBIRTH", "MQTT Client Info")]
    clients = [r for r in diag if r["edge_node_id"] == "MQTT Client Info" and r["message_type"] == "DDATA"]
    assert clients and '"name":"ClientID","timestamp"' in clients[-1]["msg"] and '"value":"diag-host"' in clients[-1]["msg"]
    assert '"name":"Connected","timestamp"' in clients[-1]["msg"] and '"dataType":"String","value":"true"' in clients[-1]["msg"]
    bridge_row = [r for r in diag if r["edge_node_id"] == "Bridge Info" and r["message_type"] == "DDATA"][-1]
    assert '"name":"ConnectedEdgeNodes","timestamp"' in bridge_row["msg"]
    assert json.loads(bridge_row["msg"])["metrics"][0]["value"]["metrics"][2]["value"] == 1      # Line A online
    # the NBIRTH call for the source's edge node and the three diagnostics edge nodes, all checked
    s = bridge.birth_notifiers["sp"].status()
    assert (s.calls, s.checked, s.failures) == (4, 4, 0)
    assert sorted(made[0].lookups) == sorted([("c", "nb_plant1_line_a"), ("c", "nb_bridge_bridge_info"),
                                              ("c", "nb_bridge_edge_node_info"), ("c", "nb_bridge_mqtt_client_info")])


async def test_diagnostics_are_off_by_default(tmp_path, fake_streams):
    bridge = core_bridge.Bridge(bridge_config(tmp_path))
    await bridge.start_infra()
    try:
        await asyncio.sleep(0.2)
    finally:
        await bridge.stop()
    assert bridge.diagnostics is None and not [r for r in _written(fake_streams) if r["group_id"] == "BRIDGE"]
