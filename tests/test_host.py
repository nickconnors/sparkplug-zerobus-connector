"""HostState, the STATE helpers and the HostSupervisor's gate."""
from __future__ import annotations

import asyncio
import json

from sparkplug_connector.core.confirmed_writer import WriterStatus
from sparkplug_connector.sparkplug.host import (
    HostState,
    HostSupervisor,
    is_online_state,
    state_payload,
    state_topic,
)

T0 = 1_790_694_000.0
ONLINE, OFFLINE = HostState.ONLINE, HostState.OFFLINE


def t(seconds: float) -> float:
    return T0 + seconds


def status(failing_since=None, pending_since=None, last_success=None, high_water=False, stream_open=True) -> WriterStatus:
    return WriterStatus(stream_open=stream_open, failing_since=failing_since, pending_since=pending_since, last_success=last_success,
                        written=0, failures=0, batch_in_flight=0, last_error=None, rows_too_large=0, queue_depth=0,
                        queue_high_water=high_water)


def healthy(last_success):
    return status(last_success=t(last_success))


def failing(since):
    return status(failing_since=t(since), last_success=t(-100), stream_open=False)


def pending(since):
    return status(pending_since=t(since), last_success=t(-100))


def test_starts_offline_until_zerobus_is_first_healthy():
    s = HostState(30, 30)
    assert s.mode == OFFLINE
    assert s.evaluate(status(stream_open=False), t(0)) == OFFLINE
    assert s.evaluate(failing(0), t(1)) == OFFLINE
    assert s.evaluate(healthy(2), t(2)) == ONLINE          # first health: no wait


def test_short_failures_dont_take_it_offline():
    s = HostState(30, 30)
    s.evaluate(healthy(0), t(0))
    assert s.evaluate(failing(1), t(10)) == ONLINE
    assert s.evaluate(failing(1), t(30)) == ONLINE         # 29 s
    assert s.evaluate(healthy(31), t(31)) == ONLINE


def test_goes_offline_after_failing_for_offline_after():
    s = HostState(30, 30)
    s.evaluate(healthy(0), t(0))
    assert s.evaluate(failing(1), t(31)) == OFFLINE


def test_goes_offline_when_a_batch_waits_too_long_for_its_ack():
    s = HostState(30, 30)
    s.evaluate(healthy(0), t(0))
    assert s.evaluate(pending(5), t(20)) == ONLINE
    assert s.evaluate(pending(5), t(35)) == OFFLINE


def test_comes_back_only_after_being_healthy_for_online_after():
    s = HostState(30, 30)
    s.evaluate(healthy(0), t(0))
    s.evaluate(failing(1), t(40))                          # OFFLINE
    assert s.evaluate(healthy(50), t(50)) == OFFLINE
    assert s.evaluate(healthy(50), t(79)) == OFFLINE       # 29 s healthy
    assert s.evaluate(healthy(50), t(80)) == ONLINE        # 30 s healthy


def test_a_failure_during_recovery_restarts_the_clock():
    s = HostState(30, 30)
    s.evaluate(healthy(0), t(0))
    s.evaluate(failing(1), t(40))                          # OFFLINE
    s.evaluate(healthy(50), t(50))
    s.evaluate(failing(70), t(70))                         # blip
    assert s.evaluate(healthy(71), t(90)) == OFFLINE       # healthy only since 90
    assert s.evaluate(healthy(71), t(119)) == OFFLINE
    assert s.evaluate(healthy(71), t(120)) == ONLINE


def test_a_stuck_batch_is_not_healthy():
    s = HostState(30, 30)
    s.evaluate(healthy(0), t(0))
    s.evaluate(pending(1), t(40))                          # OFFLINE: stuck
    assert s.evaluate(pending(1), t(100)) == OFFLINE


def test_a_queue_above_its_high_water_mark_goes_offline_at_once_and_back_after_online_after():
    # rows arriving faster than they're written must not be dropped while ONLINE
    s = HostState(30, 30)
    s.evaluate(healthy(0), t(0))
    assert s.evaluate(status(last_success=t(1), high_water=True), t(1)) == OFFLINE
    assert s.evaluate(healthy(2), t(2)) == OFFLINE
    assert s.evaluate(healthy(2), t(32)) == ONLINE


def test_no_writer_is_offline_at_once_and_back_after_online_after():
    # Zerobus stopped (no writer): nothing is written, so the edges must buffer
    s = HostState(30, 30)
    assert s.evaluate(None, t(0)) == OFFLINE                  # never ONLINE without a writer
    s.evaluate(healthy(1), t(1))
    assert s.evaluate(None, t(2)) == OFFLINE
    assert s.evaluate(healthy(3), t(3)) == OFFLINE
    assert s.evaluate(healthy(3), t(33)) == ONLINE


# ----------------------------------------------------------------------------- STATE

def test_state_topics_and_payloads():
    assert state_topic("v3", "databricks-zerobus") == "spBv1.0/STATE/databricks-zerobus"
    assert state_topic("v2", "databricks-zerobus") == "STATE/databricks-zerobus"
    assert json.loads(state_payload("v3", True, 1790694245123)) == {"online": True, "timestamp": 1790694245123}
    assert state_payload("v3", False, 5) == b'{"online":false,"timestamp":5}'
    assert (state_payload("v2", True, 5), state_payload("v2", False, 5)) == (b"ONLINE", b"OFFLINE")
    assert is_online_state("v3", b'{"online":true,"timestamp":1}') and not is_online_state("v3", b'{"online":false}')
    assert not is_online_state("v3", b"not json") and is_online_state("v2", b"ONLINE ") and not is_online_state("v2", b"OFFLINE")


# ----------------------------------------------------------------------------- supervisor

class FakeHost:
    def __init__(self):
        self.online_allowed = asyncio.Event()
        self.connected = False
        self.offlines = 0

    @property
    def is_connected(self):
        return self.connected

    async def go_offline(self):
        self.offlines += 1
        self.connected = False


async def test_supervisor_opens_the_gate_when_online_and_takes_the_host_offline():
    host, now = FakeHost(), [t(0)]
    current = [status(stream_open=False)]
    sup = HostSupervisor(host, lambda: current[0], HostState(30, 30), clock=lambda: now[0])
    assert await sup.tick() == OFFLINE and not host.online_allowed.is_set()
    current[0] = healthy(1)
    now[0] = t(1)
    assert await sup.tick() == ONLINE and host.online_allowed.is_set()
    host.connected = True
    current[0] = failing(2)
    now[0] = t(40)
    assert await sup.tick() == OFFLINE
    assert not host.online_allowed.is_set() and host.offlines == 1 and not host.connected
    assert await sup.tick() == OFFLINE and host.offlines == 1             # already disconnected


async def test_the_supervisor_carries_on_after_a_tick_that_raises():
    """A supervisor task that ended would leave the gate as it was for good (e.g. ONLINE while writes fail)."""
    host, calls = FakeHost(), []

    def writer_status():
        calls.append(1)
        if len(calls) <= 2:
            raise RuntimeError("queue depth unreadable")
        return healthy(1)
    sup = HostSupervisor(host, writer_status, HostState(30, 0), tick_s=0.01, clock=lambda: t(2))
    task = asyncio.create_task(sup.run())
    try:
        await asyncio.wait_for(host.online_allowed.wait(), 2)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(calls) >= 3
