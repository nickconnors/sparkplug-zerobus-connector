"""Zerobus stopped and started again on a running connector (the web page's ZeroBus switch) with confirmed
writes: no row lost or left outstanding in the write-ahead log, the primary host OFFLINE while there is no
writer, and the NBIRTH caller on the new writer; an acknowledged NBIRTH through to its procedure call."""
from __future__ import annotations

import asyncio
import json

import pytest

from conftest import FIXTURES
from test_edge_node_schemas import FakeSql
from test_per_target_queues import rows, wait_for
from test_sparkplug_source import broker, publish  # noqa: F401 - broker: a fixture
from test_wal import bridge_config, fake_streams, ids  # noqa: F401 - fake_streams is a fixture
from unified_connector.core import unified_bridge
from unified_connector.protocols.base import ProtocolType
from unified_connector.sparkplug.raw_row import convert

TARGET = "example.c.s.sparkplug_raw"
NBIRTH = (FIXTURES / "transmission" / "nbirth-1.bin").read_bytes()


def written(streams) -> list[str]:
    """msg_ids of the batches the fake streams acknowledged (a failed wait leaves its batch unacknowledged)."""
    return ids(json.loads(j) for batch in streams.acked for j in batch)


@pytest.fixture
def acked(fake_streams):
    """Records the batches whose wait_for succeeded."""
    fake_streams.acked = []
    original = fake_streams.open

    async def open_():
        stream = await original()
        wait = stream.wait_for

        async def wait_for_(offset):
            await wait(offset)
            fake_streams.acked.append(fake_streams.ingested[offset])
        stream.wait_for = wait_for_
        return stream
    fake_streams.open = open_
    return fake_streams


async def test_rows_queued_while_zerobus_is_stopped_are_written_once_after_it_starts(tmp_path, acked):
    acked.fail_opens = 10**6                              # unreachable: the 3 rows stay queued
    bridge = unified_bridge.UnifiedBridge(bridge_config(tmp_path))
    await bridge.start_infra()
    first = rows("A", 3)
    for r in first:
        bridge._on_record(r)
    assert (await bridge.stop_zerobus())["status"] == "ok"
    acked.fail_opens = 0
    assert (await bridge.start_zerobus())["status"] == "ok"
    later = rows("A", 1)
    bridge._on_record(later[0])
    await bridge.stop()
    assert written(acked) == [r.msg_id for r in first + later]
    assert len(bridge.writers) == 1
    assert bridge.get_metrics()["wal"][TARGET]["pending"] == 0


async def test_a_batch_in_flight_when_zerobus_stops_is_sent_first_by_the_next_writer(tmp_path, acked):
    acked.fail_waits = 10**6                              # sent, never acknowledged
    bridge = unified_bridge.UnifiedBridge(bridge_config(tmp_path))
    await bridge.start_infra()
    sent = rows("A", 3)
    for r in sent:
        bridge._on_record(r)
    await wait_for(lambda: acked.ingested)
    await bridge.stop_zerobus()
    assert acked.acked == []
    acked.fail_waits = 0
    await bridge.start_zerobus()
    later = rows("A", 1)
    bridge._on_record(later[0])
    await bridge.stop()
    assert written(acked) == [r.msg_id for r in sent + later]
    status = bridge.get_metrics()["wal"][TARGET]
    assert status["pending"] == 0 and status["checkpoint"] == 4


class FakeHost:
    """A Sparkplug source as the bridge drives it: the connect gate and OFFLINE, no MQTT."""
    protocol_type = ProtocolType.SPARKPLUG

    def __init__(self, config):
        self.config = config
        self.primary_host_id = config.get("primary_host_id") or None
        self.offline_after_seconds = 30
        self.online_after_seconds = config.get("online_after_seconds", 0)
        self.endpoint = config.get("endpoint")
        self.online_allowed = asyncio.Event()
        self.is_connected = True
        self.offline_calls = 0
        self._stopped = asyncio.Event()

    async def go_offline(self):
        self.offline_calls += 1
        self.is_connected = False

    async def connect(self):
        await self._stopped.wait()                          # "connected" until the bridge stops

    async def subscribe(self):
        pass

    async def stop(self):
        self._stopped.set()

    async def disconnect(self):
        pass


@pytest.fixture
def hosts(monkeypatch):
    made: dict[str, FakeHost] = {}

    def factory(protocol_type, source_name, endpoint, config, on_record, on_stats):
        made[source_name] = FakeHost(config)
        return made[source_name]

    monkeypatch.setattr(unified_bridge, "create_protocol_client", factory)
    return made


def sparkplug_source(**extra) -> dict:
    return {"name": "sp", "protocol": "sparkplug", "endpoint": "mqtt://localhost:1883",
            "primary_host_id": "databricks-zerobus", "online_after_seconds": 0, **extra}


async def test_the_primary_host_is_offline_while_zerobus_is_stopped(tmp_path, acked, hosts):
    cfg = bridge_config(tmp_path)
    cfg["sources"] = [sparkplug_source()]
    bridge = unified_bridge.UnifiedBridge(cfg)
    await bridge.start()
    supervisor = bridge.host_supervisors["sp"]
    await wait_for(lambda: bridge.writers[TARGET].status().last_success is not None)
    assert await supervisor.tick() == "ONLINE"
    await bridge.stop_zerobus()
    assert await supervisor.tick() == "OFFLINE" and hosts["sp"].offline_calls == 1
    await bridge.start_zerobus()
    await wait_for(lambda: bridge.writers[TARGET].status().last_success is not None)
    assert await supervisor.tick() == "ONLINE"            # online_after_seconds 0
    await bridge.stop()


async def test_a_primary_host_started_before_zerobus_waits_offline_for_its_writer(tmp_path, acked, hosts):
    cfg = bridge_config(tmp_path)
    cfg["zerobus"]["enabled"] = False                       # data collection only, Zerobus started later
    cfg["sources"] = [sparkplug_source()]
    bridge = unified_bridge.UnifiedBridge(cfg)
    await bridge.start()
    supervisor = bridge.host_supervisors["sp"]
    assert await supervisor.tick() == "OFFLINE" and not hosts["sp"].online_allowed.is_set()
    await bridge.start_zerobus()
    await wait_for(lambda: bridge.writers[TARGET].status().last_success is not None)
    assert await supervisor.tick() == "ONLINE" and hosts["sp"].online_allowed.is_set()
    await bridge.stop()


async def test_without_confirmed_writes_a_sparkplug_table_still_has_them_and_the_host_follows_it(tmp_path, acked, hosts):
    cfg = bridge_config(tmp_path)
    cfg["zerobus"].update({"enabled": False, "confirmed_writes": False})
    cfg["sources"] = [sparkplug_source()]
    bridge = unified_bridge.UnifiedBridge(cfg)
    await bridge.start()
    supervisor = bridge.host_supervisors["sp"]
    assert await supervisor.tick() == "OFFLINE"            # no writer yet: nothing is stored
    await bridge.start_zerobus()
    assert TARGET in bridge.writers                         # the table gets Sparkplug rows: confirmed writes
    await wait_for(lambda: bridge.writers[TARGET].status().last_success is not None)
    assert await supervisor.tick() == "ONLINE"
    await bridge.stop()


@pytest.mark.parametrize("catalog, calls", [("c-1", True), ("c`x", False)])
async def test_a_raw_table_name_the_nbirth_caller_cant_use_doesnt_stop_zerobus(tmp_path, acked, hosts, catalog, calls):
    cfg = bridge_config(tmp_path)
    cfg["zerobus"]["enabled"] = False
    cfg["zerobus"]["default_target"]["catalog"] = catalog
    cfg["sources"] = [sparkplug_source(edge_node_schemas={"enabled": True, "warehouse_id": "w",
                                                          "procedures": ["c.s.create_edge_node_schema"]})]
    bridge = unified_bridge.UnifiedBridge(cfg)
    await bridge.start()
    assert (await bridge.start_zerobus())["status"] == "ok"
    assert len(bridge.writers) == 1 and len(bridge.batch_tasks) == 1     # Zerobus runs either way
    assert ("sp" in bridge.birth_notifiers) is calls      # a hyphen is fine (quoted); a backquote isn't
    await bridge.stop()


@pytest.mark.parametrize("zerobus_at_start", [True, False])
async def test_the_nbirth_caller_listens_on_the_writer_of_the_moment(tmp_path, acked, hosts, zerobus_at_start):
    cfg = bridge_config(tmp_path)
    cfg["zerobus"]["enabled"] = zerobus_at_start
    cfg["sources"] = [sparkplug_source(edge_node_schemas={"enabled": True, "warehouse_id": "w",
                                                          "procedures": ["c.s.create_edge_node_schema"]})]
    bridge = unified_bridge.UnifiedBridge(cfg)
    await bridge.start()
    if zerobus_at_start:
        old = bridge.writers[TARGET]
        notifier = bridge.birth_notifiers["sp"]
        assert notifier.accept in old._listeners
        await bridge.stop_zerobus()
        assert notifier.accept not in old._listeners
    else:
        assert bridge.birth_notifiers == {}                 # no writer yet
    await bridge.start_zerobus()
    notifier = bridge.birth_notifiers["sp"]
    assert bridge.writers[TARGET]._listeners.count(notifier.accept) == 1
    await bridge.start_zerobus()                            # already running: nothing added twice
    assert bridge.writers[TARGET]._listeners.count(notifier.accept) == 1
    await bridge.stop()


async def test_an_acknowledged_nbirth_calls_the_procedure(tmp_path, acked, hosts, monkeypatch):
    """The whole way: the row through the confirmed writer, its acknowledgement, the NBIRTH caller's check that
    the row is readable, the CALL (the caller picks NBIRTH out of the JSON text the writer sent)."""
    made = []

    class Sql(FakeSql):
        def __init__(self, *args):
            super().__init__()
            made.append(self)
    monkeypatch.setattr(unified_bridge, "DatabricksSql", Sql)
    cfg = bridge_config(tmp_path)
    cfg["sources"] = [sparkplug_source(edge_node_schemas={"enabled": True, "warehouse_id": "w",
                                                          "procedures": ["c.s.create_edge_node_schema"]})]
    bridge = unified_bridge.UnifiedBridge(cfg)
    await bridge.start()
    try:
        ndeath = rows("A", 1)[0]
        nbirth = convert("spBv1.0/Plant1/NBIRTH/Line A", NBIRTH, 1790694245123).row
        bridge._on_record(ndeath, "sp")                     # no call
        bridge._on_record(nbirth, "sp")
        await wait_for(lambda: made and made[0].calls(), 10)
    finally:
        await bridge.stop()
    assert written(acked) == [ndeath.msg_id, nbirth.msg_id]
    assert made[0].log == [f"check {nbirth.msg_id}", "CALL c.s.create_edge_node_schema(nb, Plant1, Line A, false)"]


async def test_a_sparkplug_protocol_written_with_capitals_is_still_a_primary_host(tmp_path, acked, hosts):
    """The factory takes the protocol in any case; a primary host without its supervisor waits to go ONLINE for good."""
    cfg = bridge_config(tmp_path)
    cfg["sources"] = [sparkplug_source(protocol="Sparkplug")]
    bridge = unified_bridge.UnifiedBridge(cfg)
    await bridge.start()
    try:
        assert "sp" in bridge.host_supervisors
    finally:
        await bridge.stop()


@pytest.mark.parametrize("session_s, at_most", [(0.0, 8), (0.06, None)])
async def test_the_reconnect_backoff_grows_unless_the_session_ran_a_while(monkeypatch, session_s, at_most):
    """A session that fails right after each connect (e.g. the broker refuses the STATE publish) backs off more each
    time; one that ran for HEALTHY_SESSION_S starts the backoff again from the initial delay."""
    connects = []

    class Flaky:
        protocol_type, primary_host_id, endpoint = ProtocolType.MQTT, None, "mqtt://localhost:1883"

        async def connect(self):
            connects.append(1)

        async def subscribe(self):
            await asyncio.sleep(session_s)
            raise ConnectionError("not authorized")

        async def stop(self):
            pass

        async def disconnect(self):
            pass

    monkeypatch.setattr(unified_bridge, "create_protocol_client", lambda *a, **k: Flaky())
    monkeypatch.setattr(unified_bridge, "HEALTHY_SESSION_S", 0.05)
    bridge = unified_bridge.UnifiedBridge({"sources": [{"name": "m", "protocol": "mqtt", "endpoint": "mqtt://localhost:1883"}],
                                           "zerobus": {"enabled": False}, "normalization": {"enabled": False},
                                           "backpressure": {"disk_spool": {"enabled": False, "encryption": False}}})
    bridge.reconnect_initial_delay_sec, bridge.reconnect_multiplier, bridge.reconnect_max_delay_sec = 0.01, 2.0, 10.0
    await bridge.start()
    await asyncio.sleep(0.6)
    await bridge.stop()
    if at_most:
        assert len(connects) <= at_most                    # 0.01, 0.02, 0.04, ... s apart
    else:
        assert len(connects) >= 5                          # every 0.06 + 0.01 s


async def test_the_bridge_stops_while_a_primary_host_waits_to_go_online(tmp_path):
    """No writer (Zerobus off): the source waits in connect() for the gate; stop() must cancel it, not hang."""
    cfg = bridge_config(tmp_path)
    cfg["zerobus"]["enabled"] = False
    cfg["sources"] = [sparkplug_source()]                   # the real SparkplugClient (nothing listens: it never connects)
    bridge = unified_bridge.UnifiedBridge(cfg)
    await bridge.start()
    await asyncio.sleep(0.1)
    task = bridge.client_tasks["sp"]
    assert not task.done() and not bridge.clients["sp"].online_allowed.is_set()
    await asyncio.wait_for(bridge.stop(), 10)
    assert task.done()


async def test_the_sparkplug_health_of_a_primary_host_and_of_a_plain_subscriber(tmp_path, acked, broker, monkeypatch):
    made = []

    class Sql(FakeSql):
        def __init__(self, *args):
            super().__init__()
            made.append(self)
    monkeypatch.setattr(unified_bridge, "DatabricksSql", Sql)
    endpoint, port = broker
    cfg = bridge_config(tmp_path)
    cfg["sources"] = [sparkplug_source(endpoint=endpoint, client_id="health-host", edge_node_schemas={
                          "enabled": True, "warehouse_id": "w", "procedures": ["c.s.create_edge_node_schema"]}),
                      {"name": "plain", "protocol": "sparkplug", "endpoint": endpoint, "client_id": "health-plain",
                       "topics": ["spBv1.0/Other/#"]}]
    bridge = unified_bridge.UnifiedBridge(cfg)
    await bridge.start()
    try:
        await wait_for(lambda: all(n in bridge.clients and bridge.clients[n].subscribed.is_set() for n in ("sp", "plain")), 10)
        await publish(port, [("spBv1.0/Plant1/NBIRTH/Line A", NBIRTH)])
        await wait_for(lambda: made and made[0].calls(), 15)
        health = bridge.sparkplug_health()
    finally:
        await bridge.stop()
    sp, plain = health["sp"], health["plain"]
    assert {k: sp[k] for k in ("state", "primary_host_id", "mqtt_connected", "mqtt_online", "rows_queued", "rows_failed",
                               "rows_written", "rows_unconvertible", "batch_in_flight", "queue_dropped", "wal_pending",
                               "wal_rows_appended", "nbirths_written", "edge_node_schema_calls",
                               "edge_node_schema_failures")} == {
        "state": "ONLINE", "primary_host_id": "databricks-zerobus", "mqtt_connected": True, "mqtt_online": True,
        "rows_queued": 1, "rows_failed": 0, "rows_written": 1, "rows_unconvertible": 0, "batch_in_flight": 0,
        "queue_dropped": 0, "wal_pending": 0, "wal_rows_appended": 1, "nbirths_written": 1, "edge_node_schema_calls": 1,
        "edge_node_schema_failures": 0}
    assert sp["state_topic"] == "spBv1.0/STATE/databricks-zerobus" and sp["zerobus_last_success"].endswith("Z")
    assert (plain["state"], plain["primary_host_id"], plain["rows_queued"]) == ("ONLINE", None, 0)   # connected
