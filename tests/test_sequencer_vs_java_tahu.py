"""The sequencer against Tahu 1.0.21's own host classes (``java_tahu.TahuHost``).

The same messages go to both; the rows (topic and JSON, in order: Tahu's are the payloads it hands to the event
handler) and the rebirth requests must be equal. Random sequences (hypothesis) of births, deaths and data with
skipped, repeated and out-of-order ``seq``, wrong ``bdSeq``, missing fields, aliases and metrics without a
datatype; the timeout with Tahu's real timer in two scenarios. Skipped without Java 17 and ``tahu-host-1.0.21.jar``.
"""
from __future__ import annotations

import time
import uuid

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from sparkplug_connector.sparkplug import sparkplug_b_pb2 as pb
from sparkplug_connector.sparkplug.raw_row import convert
from sparkplug_connector.sparkplug.sequencer import Sequencer, rebirth_topic

TS = 1790696925611


@pytest.fixture(scope="module")
def tahu(java_tahu):
    reason = java_tahu.host_unavailable_reason()
    if reason:
        pytest.skip(reason)
    return java_tahu


class Both:
    """Tahu's host and the sequencer side by side, in a group of their own (Tahu's state is global)."""

    def __init__(self, tahu, timeout_ms=None):
        self.group = f"G{uuid.uuid4().hex[:10]}"
        self.host = tahu.TahuHost(timeout_ms)
        self.t0 = time.monotonic()                  # Tahu's timer started: it ticks at t0 + 1 s, + 2 s, ...
        self.requests: list[str] = []
        self.seq = Sequencer(reorder_timeout_ms=timeout_ms or 5000,
                             request_rebirth=lambda g, e: self.requests.append(rebirth_topic(g, e)))
        self.rows: list[tuple[str, str]] = []
        self.ticks = 0

    def send(self, kind, edge, device, payload: bytes):
        self._ticks()
        topic = f"spBv1.0/{self.group}/{kind}/{edge}" + (f"/{device}" if kind[0] == "D" else "")
        self.host.send(topic, payload)
        outcome = convert(topic, payload, 0)
        if outcome.row is not None:
            self.rows += [(r.msg_topic, r.msg) for r in self.seq.on_message(outcome, time.monotonic())]

    def wait_until(self, offset_s):
        """Sleep until t0 + offset (the sequencer ticks when Tahu's timer does: t0 + 1 s, + 2 s, ...)."""
        time.sleep(max(0.0, self.t0 + offset_s - time.monotonic()))
        self._ticks()

    def _ticks(self):
        while self.t0 + self.ticks + 1 <= time.monotonic():
            self.ticks += 1
            self.seq.tick(self.t0 + self.ticks)

    def compare(self):
        self.host.drain()
        self.host.close()
        assert self.rows == self.host.rows
        assert sorted(self.requests) == sorted(self.host.requests)   # one tick's requests come in Tahu's hash order


def msg(seq=None, bdseq=None, ts=None, metrics=()):
    p = pb.Payload(timestamp=ts or int(time.time() * 1000))
    if seq is not None:
        p.seq = seq
    if bdseq is not None:
        p.metrics.add(name="bdSeq", datatype=4, long_value=bdseq)
    for m in metrics:
        p.metrics.append(m)
    return p.SerializeToString()


def m(name, datatype, alias=None, **value):
    out = pb.Payload.Metric(datatype=datatype, **value)
    if name is not None:
        out.name = name
    if alias is not None:
        out.alias = alias
    return out


# ----------------------------------------------------------------------------- fixed scenarios (no timeout)

SPEED, MOTOR = m("Speed", 9, float_value=1.0), m("Motor", 19, template_value=pb.Payload.Template(
    template_ref="M", metrics=[m("rpm", 3, int_value=1)]))
SCENARIOS = {
    "in order": [("NBIRTH", 0, 0), ("DBIRTH", 1), ("NDATA", 2), ("DDATA", 3), ("DDEATH", 4), ("NDEATH", None, 0)],
    "swapped": [("NBIRTH", 0, 0), ("DBIRTH", 1), ("DDATA", 3), ("DDATA", 2), ("DDATA", 4)],
    "never born": [("DDATA", 7), ("NDATA", 8), ("NDATA", 0), ("NDATA", 1)],
    "repeated seq": [("NBIRTH", 0, 0), ("DBIRTH", 1), ("DDATA", 2), ("DDATA", 2), ("DDATA", 3), ("DDATA", 5), ("DDATA", 5)],
    "ndeath": [("NDEATH", None, 5), ("NBIRTH", 0, 5), ("DBIRTH", 1), ("NDEATH", None, 4), ("NDEATH", None, 5),
               ("NDEATH", None, 5), ("DDATA", 2), ("NDATA", 3)],
    "off by one": [("NBIRTH", 0, 0), ("DBIRTH", 1), ("DDEATH", 2), ("DDEATH", 3), ("DDATA", 4), ("NDATA", 5),
                   ("NBIRTH", 0, 0), ("DBIRTH", 1), ("DDATA", 2)],
    "no seq": [("NBIRTH", 0, 0), ("DBIRTH", 1), ("DDATA", None), ("DDATA", 2)],
    "nbirth without bdseq": [("NBIRTH", 0, None), ("NDATA", 1), ("DBIRTH", 2)],
    "wrap": [("NBIRTH", 0, 0), ("DBIRTH", 1)] + [("DDATA", s % 256) for s in range(2, 260)],
}


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_scenario_like_tahu(tahu, name):
    both = Both(tahu)
    for step in SCENARIOS[name]:
        kind, seq, bdseq = (step + (None, None))[:3]
        both.send(kind, "E1", "D1", msg(seq, bdseq))
    both.compare()


def test_births_types_and_aliases_like_tahu(tahu):
    both = Both(tahu)
    both.send("NBIRTH", "E1", None, msg(0, 0, metrics=[m("Node", 3, alias=1, int_value=1)]))
    both.send("DBIRTH", "E1", "D1", msg(1, metrics=[SPEED, MOTOR, m("Aliased", 10, alias=2, double_value=1.0)]))
    data = [m("Speed", 0, float_value=2.5), m(None, 0, alias=2, double_value=0.5), m("Missing", 0, int_value=1),
            m("Motor", 19, template_value=pb.Payload.Template(template_ref="M", metrics=[m("rpm", 0, int_value=9)]))]
    both.send("DDATA", "E1", "D1", msg(2, metrics=data))
    both.send("NDATA", "E1", None, msg(3, metrics=[m(None, 0, alias=1, int_value=4)]))
    both.send("DBIRTH", "E1", "D1", msg(4, metrics=[m("Aliased", 10, alias=2, double_value=1.0)]))   # alias known: request
    both.send("NBIRTH", "E1", None, msg(0, 0, metrics=[m("Dup", 3, alias=7, int_value=1), m("Dup2", 3, alias=7, int_value=1)]))
    both.send("DDATA", "E1", "D1", msg(1, metrics=data))                # the NBIRTH cleared the device's types
    both.compare()


# ----------------------------------------------------------------------------- the timeout (Tahu's timer)

def test_a_gap_times_out_like_tahu(tahu):
    both = Both(tahu, timeout_ms=200)
    both.wait_until(0.1)
    for kind, seq in [("NBIRTH", 0), ("DBIRTH", 1), ("DDATA", 2), ("DDATA", 4)]:
        both.send(kind, "E1", "D1", msg(seq, 0 if kind == "NBIRTH" else None))
    both.wait_until(1.5)                            # timed out at 1 s: dropped, a request, the node offline
    both.send("DDATA", "E1", "D1", msg(5))
    both.send("NDATA", "E1", None, msg(0))          # seq 0 passes: the node is offline, rate-limited request
    both.wait_until(2.5)                            # 2 s: timed out again, rate-limited
    for kind, seq in [("NBIRTH", 0), ("DBIRTH", 1), ("DDATA", 2)]:
        both.send(kind, "E1", "D1", msg(seq, 0 if kind == "NBIRTH" else None))
    both.compare()
    assert len(both.requests) == 1 and len(both.rows) == 6


def test_data_before_any_birth_times_out_like_tahu(tahu):
    both = Both(tahu, timeout_ms=200)
    both.wait_until(0.1)
    both.send("DDATA", "E1", "D1", msg(7))
    both.send("NDATA", "E2", None, msg(9))
    both.wait_until(1.5)
    both.compare()
    assert len(both.requests) == 2 and not both.rows


# ----------------------------------------------------------------------------- random sequences

KINDS = ["NBIRTH", "DBIRTH", "NDATA", "DDATA", "DDATA", "DDATA", "NDEATH", "DDEATH"]
NAMES = ["Speed", "Count", "Motor"]


@st.composite
def sequences(draw):
    """Messages over two edge nodes and two devices; seq mostly next, sometimes skipped, repeated, out of order."""
    counters = {"E1": 0, "E2": 0}
    out = []
    ts = TS
    for _ in range(draw(st.integers(1, 30))):
        edge = draw(st.sampled_from(["E1", "E1", "E2"]))
        device = draw(st.sampled_from(["D1", "D1", "D2"]))
        kind = draw(st.sampled_from(KINDS))
        how = draw(st.sampled_from(["next"] * 6 + ["skip", "repeat", "back", "random", "none"]))
        if kind == "NBIRTH":
            seq = 0 if how != "random" else draw(st.integers(0, 3))
            counters[edge] = seq + 1
        elif kind == "NDEATH":
            seq = None
        else:
            seq = {"next": counters[edge], "skip": counters[edge] + 1, "repeat": counters[edge] - 1,
                   "back": counters[edge] - 2, "random": draw(st.integers(0, 255)), "none": None}[how]
            seq = None if seq is None else seq % 256
            if how in ("next", "skip"):
                counters[edge] = (seq + 1) % 256
        bdseq = draw(st.sampled_from([0, 0, 1, None])) if kind in ("NBIRTH", "NDEATH") else None
        ts += draw(st.sampled_from([1, 5, -3]))
        metrics = []
        for name in draw(st.lists(st.sampled_from(NAMES), max_size=3, unique=True)):
            birth = kind.endswith("BIRTH")
            datatype = 19 if name == "Motor" else 3
            if not birth and draw(st.booleans()):
                datatype = 0                        # a metric sent without its datatype
            alias = draw(st.sampled_from([None, None, 1, 2]))
            if name == "Motor":
                member = m("rpm", 3 if birth or draw(st.booleans()) else 0, int_value=1)
                metrics.append(m(name, datatype, alias=alias, template_value=pb.Payload.Template(template_ref="M", metrics=[member])))
            else:
                metrics.append(m(None if alias and not birth and draw(st.booleans()) else name, datatype, alias=alias, int_value=2))
        out.append((kind, edge, device, msg(seq, bdseq, ts, metrics)))
    return out


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
@given(sequences())
def test_random_sequences_like_tahu(tahu, messages):
    both = Both(tahu)
    for kind, edge, device, payload in messages:
        both.send(kind, edge, device, payload)
    both.compare()
