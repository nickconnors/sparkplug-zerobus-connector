"""The sequencer: Tahu 1.0.21's host decisions on rows and rebirth requests, with a fake clock.

The cases cover Tahu's rules (gaps, swaps, the wrap, repeated seq, births, deaths, aliases, the quirks);
``test_sequencer_vs_java_tahu.py`` checks the same against Tahu itself.
"""
from __future__ import annotations

import json

import pytest

from sparkplug_connector.sparkplug import sparkplug_b_pb2 as pb
from sparkplug_connector.sparkplug.raw_row import convert
from sparkplug_connector.sparkplug.sequencer import KEEP_ALL, Sequencer, rebirth_payload, rebirth_topic

GROUP, EDGE, DEV = "Plant1", "Line A", "M100"
TS = 1790696925611


class Run:
    """A sequencer fed messages at chosen times; rows by label, rebirth requests as (group, edge, time)."""

    def __init__(self, **settings):
        self.now = 0.0
        self.requests: list[tuple[str, str, float]] = []
        self.s = Sequencer(request_rebirth=lambda g, e: self.requests.append((g, e, self.now)), **settings)
        self.rows = []
        self.n = 0

    def send(self, kind, seq=None, bdseq=None, at=None, device=DEV, edge=EDGE, ts=None, metrics=(), label=None):
        """Publish one message; returns the labels of the rows written now."""
        if at is not None:
            self.now = at
        self.n += 1
        label = label or f"{kind}#{self.n}"
        p = pb.Payload(timestamp=TS + self.n if ts is None else ts)
        if seq is not None:
            p.seq = seq
        if bdseq is not None:
            p.metrics.add(name="bdSeq", datatype=4, long_value=bdseq)
        for m in metrics:
            p.metrics.append(m)
        if kind != "DDEATH":
            p.metrics.add(name="probe", datatype=12, string_value=label)
        topic = f"spBv1.0/{GROUP}/{kind}/{edge}" + (f"/{device}" if kind[0] == "D" else "")
        rows = self.s.on_message(convert(topic, p.SerializeToString(), int(self.now * 1000)), self.now)
        self.rows += rows
        return [_label(r) for r in rows]

    def tick(self, at):
        self.now = at
        self.s.tick(at)

    def labels(self):
        return [_label(r) for r in self.rows]

    def requested_at(self):
        return [t for _, _, t in self.requests]


def _label(row):
    msg = json.loads(row.msg)
    return next((m["value"] for m in msg["metrics"] if m and m.get("name") == "probe"), f"{row.message_type}:seq{msg.get('seq')}")


def born(r: Run, at=0.0, bdseq=0):
    """NBIRTH 0 and DBIRTH 1."""
    r.send("NBIRTH", 0, bdseq=bdseq, at=at, label="NBIRTH")
    r.send("DBIRTH", 1, label="DBIRTH")


# ----------------------------------------------------------------------------- reordering

def test_in_order_every_message_gets_its_row_at_once():
    r = Run()
    born(r)
    assert r.send("NDATA", 2, label="n2") == ["n2"]
    assert r.send("DDATA", 3, label="d3") == ["d3"]
    assert r.labels() == ["NBIRTH", "DBIRTH", "n2", "d3"] and not r.requests


def test_out_of_order_is_held_then_written_in_order_stamped_when_released():
    r = Run()
    born(r)
    assert r.send("DDATA", 3, at=1.0, label="d3") == []
    assert r.s.stats()["messages_held"] == 1
    assert r.send("DDATA", 2, at=1.5, label="d2") == ["d2", "d3"]
    assert r.rows[-1].inserted_at == 1500                       # released with d2
    assert r.s.stats()["messages_reordered"] == 1 and r.s.stats()["messages_held"] == 0


def test_a_gap_drops_the_held_after_the_timeout_and_asks_for_a_rebirth():
    r = Run()
    born(r)
    r.send("DDATA", 2, at=0.4)
    r.send("DDATA", 4, at=0.6)
    r.send("DDATA", 5, at=0.8)
    r.tick(5.4)                                                 # 5.0 s after the last in-order message: not yet
    assert not r.requests
    r.tick(5.41)
    assert r.requests == [(GROUP, EDGE, 5.41)]
    s = r.s.stats()
    assert s["dropped_gap_timeout"] == 2 and s["messages_held"] == 0
    # the edge node is set offline; the old session goes on: held (expected 0), dropped at the next timeout
    r.send("DDATA", 6, at=6.0)
    r.tick(10.41)
    assert r.requested_at() == [5.41]                           # not 5 s after the reset yet
    r.tick(10.42)
    assert r.requested_at() == [5.41, 10.42]
    # the new session
    assert r.send("NBIRTH", 0, bdseq=0, at=11.0, label="NBIRTH'") == ["NBIRTH'"]
    assert r.send("DBIRTH", 1, label="DBIRTH'") == ["DBIRTH'"]
    assert r.send("DDATA", 2, label="d2'") == ["d2'"]


def test_the_nbirth_prunes_held_messages_older_than_itself():
    r = Run()
    born(r)
    r.send("DDATA", 4, at=1.0, ts=TS + 100, label="old")
    r.send("NBIRTH", 0, bdseq=0, at=2.0, ts=TS + 200, label="NBIRTH'")
    assert r.s.stats()["dropped_pruned_at_nbirth"] == 1 and r.s.stats()["messages_held"] == 0


def test_tahu_quirk_a_held_message_newer_than_the_nbirth_joins_the_new_session():
    r = Run()
    born(r)
    r.send("DDATA", 1, at=1.0, ts=TS + 500, label="held 1")     # held: expected 2
    assert r.send("NBIRTH", 0, bdseq=0, at=2.0, ts=TS + 200, label="NBIRTH'") == ["NBIRTH'", "held 1"]


def test_a_message_from_an_edge_node_never_born_is_held_and_a_rebirth_asked():
    r = Run()
    r.send("DDATA", 7, at=0.0)
    r.send("NDATA", 8, at=0.5)
    r.tick(5.01)
    assert r.requests == [(GROUP, EDGE, 5.01)] and not r.rows
    assert r.s.stats()["dropped_gap_timeout"] == 2


def test_the_wrap_from_255_to_0():
    r = Run()
    born(r)
    for seq in list(range(2, 256)) + [0, 1, 2]:
        r.send("DDATA", seq, at=1.0)
    assert len(r.rows) == 2 + 254 + 3 and r.s.stats()["messages_dropped"] == 0


def test_a_repeated_seq_is_held_and_dropped_at_the_timeout():
    r = Run()
    born(r)
    r.send("DDATA", 2, at=0.4, label="d2 a")
    r.send("DDATA", 2, at=0.6, label="d2 b")
    r.send("DDATA", 3, at=0.8, label="d3")
    r.tick(5.81)
    assert r.labels() == ["NBIRTH", "DBIRTH", "d2 a", "d3"] and r.requested_at() == [5.81]


def test_a_second_held_message_with_the_same_seq_replaces_the_first():
    r = Run()
    born(r)
    r.send("DDATA", 5, label="first")
    r.send("DDATA", 5, label="second")
    assert r.s.stats()["dropped_replaced_seq"] == 1
    r.send("DDATA", 2)
    r.send("DDATA", 3)
    assert r.send("DDATA", 4, label="d4") == ["d4", "second"]


def test_tahu_quirk_a_stale_seq_is_handled_one_round_later():
    r = Run()
    born(r)
    r.send("DDATA", 2, label="d2")
    r.send("DDATA", 2, label="stale")                          # held: expected 3
    for seq in list(range(3, 256)) + [0]:
        r.send("DDATA", seq)
    assert r.send("DDATA", 1, label="d1") == ["d1", "stale"]    # the stale one in place of seq 2
    assert r.send("DDATA", 2, label="real d2") == []            # now held for a round


def test_a_message_without_seq_is_dropped_and_leaves_no_gap():
    r = Run()
    born(r)
    assert r.send("DDATA", None) == []
    assert r.send("DDATA", 2, label="d2") == ["d2"]
    assert r.s.stats()["dropped_no_seq"] == 1 and not r.requests


def test_an_nbirth_with_a_seq_other_than_0_is_held():
    r = Run()
    assert r.send("NBIRTH", 3, bdseq=0) == []
    assert r.s.stats()["messages_held"] == 1


# ----------------------------------------------------------------------------- state checks

def test_data_from_an_unknown_edge_node_with_seq_0_asks_at_once_rate_limited():
    r = Run()
    r.send("NDATA", 0, at=0.0)
    r.send("NDATA", 1, at=0.5)
    r.send("NDATA", 2, at=1.0)
    r.send("NDATA", 3, at=6.5)
    assert r.requested_at() == [0.0, 6.5] and not r.rows
    s = r.s.stats()
    assert s["dropped_unknown_node"] == 4 and s["rebirth_requests_rate_limited"] == 2


def test_ndeath_needs_a_known_online_edge_node_and_its_births_bdseq():
    r = Run()
    assert r.send("NDEATH", bdseq=5, at=0.0) == []              # unknown
    born(r, at=0.3, bdseq=5)
    assert r.send("NDEATH", bdseq=4, label="wrong") == []
    assert r.send("NDEATH", bdseq=5, label="death") == ["death"]
    assert r.send("NDEATH", bdseq=5, label="again") == []       # offline already
    s = r.s.stats()
    assert (s["dropped_unknown_node"], s["dropped_bdseq_mismatch"], s["dropped_node_offline"]) == (1, 1, 1)
    assert not r.requests


def test_data_after_the_ndeath_asks_at_once():
    r = Run()
    born(r, bdseq=1)
    r.send("NDEATH", bdseq=1, at=0.4)
    assert r.send("DDATA", 2, at=0.6) == []
    assert r.send("NDATA", 3, at=1.1) == []
    assert r.requested_at() == [0.6]


def test_tahu_quirk_after_a_ddeath_for_an_offline_device_every_message_fails_the_seq_check():
    r = Run()
    born(r)
    assert r.send("DDEATH", 2) == ["DDEATH:seq2"]
    assert r.send("DDEATH", 3) == []                            # the device is offline: no seq check
    assert r.send("DDATA", 4) == [] and r.send("NDATA", 5) == []
    r.tick(20.0)
    s = r.s.stats()
    assert s["dropped_device_offline"] == 1 and s["dropped_seq_check"] == 2 and not r.requests
    born(r, at=21.0)
    assert r.send("DDATA", 2, label="d2") == ["d2"]


def test_a_dbirth_from_an_unknown_edge_node_is_dropped_without_a_request():
    r = Run()
    r.send("NDATA", 0)                                          # seq 0 passes the reordering
    r.requests.clear()
    r.s._last_request.clear()
    assert r.send("DBIRTH", 1) == []
    assert r.s.stats()["dropped_unknown_node"] == 2 and not r.requests


def test_tahu_quirk_a_new_device_starts_online_even_if_its_dbirth_fails():
    r = Run()
    born(r)
    r.send("DDEATH", 2)
    r.send("DDEATH", 3)                                         # off by one from here
    assert r.send("DBIRTH", 4, device="M200") == []
    born(r, at=1.0)                                             # M200 not born again
    assert r.send("DDATA", 2, device="M200", label="M200") == ["M200"]


def test_an_nbirth_without_bdseq_is_dropped_and_the_edge_node_stays_offline():
    r = Run()
    assert r.send("NBIRTH", 0) == []
    assert r.send("NDATA", 1) == []
    assert r.s.stats()["dropped_invalid_birth"] == 1 and r.s.stats()["dropped_node_offline"] == 1 and len(r.requests) == 1


def test_a_device_offline_after_its_ddeath_still_gets_ddata():
    r = Run()
    born(r)
    r.send("DDEATH", 2)
    assert r.send("DDATA", 3, label="d3") == ["d3"]


def test_a_timeout_while_rate_limited_doesnt_set_the_edge_node_offline():
    r = Run(reorder_timeout_ms=1000)
    born(r)
    alias = [pb.Payload.Metric(name="a", alias=1, datatype=3, int_value=1), pb.Payload.Metric(name="b", alias=1, datatype=3, int_value=2)]
    r.send("DBIRTH", 2, metrics=alias, at=0.1)                  # a repeated alias: a request at 0.1
    r.send("DDATA", 4, at=0.2)                                  # held
    r.tick(1.3)                                                 # timeout: rate-limited
    assert r.requested_at() == [0.1]
    assert r.send("NDEATH", bdseq=0, label="death") == ["death"]   # still online


# ----------------------------------------------------------------------------- the births' types

def _untyped(name, **value):
    return pb.Payload.Metric(name=name, datatype=0, **value)


def _metric_json(row, name):
    return next(m for m in json.loads(row.msg)["metrics"] if m and m.get("name") == name)


def test_a_metric_without_datatype_decodes_with_its_dbirths_type():
    r = Run()
    r.send("NBIRTH", 0, bdseq=0)
    r.send("DBIRTH", 1, metrics=[pb.Payload.Metric(name="Speed", datatype=9, float_value=1.0)])
    r.send("DDATA", 2, metrics=[_untyped("Speed", float_value=2.5)])
    assert _metric_json(r.rows[-1], "Speed") == {"name": "Speed", "dataType": "Float", "value": 2.5}
    # a new NBIRTH clears the device's types until its DBIRTH
    r.send("NBIRTH", 0, bdseq=0)
    r.send("DDATA", 1, metrics=[_untyped("Speed", float_value=3.5)])
    assert None in json.loads(r.rows[-1].msg)["metrics"]


def test_template_members_take_their_types_by_template_path():
    r = Run()
    member = pb.Payload.Metric(name="rpm", datatype=3, int_value=1)
    udt = pb.Payload.Metric(name="Motor", datatype=19, template_value=pb.Payload.Template(template_ref="M", metrics=[member]))
    r.send("NBIRTH", 0, bdseq=0)
    r.send("DBIRTH", 1, metrics=[udt])
    data = pb.Payload.Metric(name="Motor", datatype=19,
                             template_value=pb.Payload.Template(template_ref="M", metrics=[_untyped("rpm", int_value=7)]))
    r.send("DDATA", 2, metrics=[data])
    assert _metric_json(r.rows[-1], "Motor")["value"]["metrics"][0] == {"name": "rpm", "dataType": "Int32", "value": 7}


def test_keep_all_decodes_with_the_types_known_on_arrival():
    r = Run(sequence_handling=KEEP_ALL)
    r.send("NBIRTH", 0, bdseq=0)
    r.send("DBIRTH", 1, metrics=[pb.Payload.Metric(name="Speed", datatype=9, float_value=1.0)])
    r.send("DDATA", 2, metrics=[_untyped("Speed", float_value=2.5)])
    assert _metric_json(r.rows[-1], "Speed")["dataType"] == "Float"


def test_a_birth_repeating_an_alias_asks_for_a_rebirth_and_keeps_its_row():
    r = Run()
    r.send("NBIRTH", 0, bdseq=0, at=0.0)
    m = [pb.Payload.Metric(name="a", alias=1, datatype=3, int_value=1)]
    assert r.send("DBIRTH", 1, metrics=m, at=0.1, label="first") == ["first"]
    assert r.send("DBIRTH", 2, metrics=m, at=6.0, label="again") == ["again"]   # no NBIRTH between: alias 1 known
    assert r.requested_at() == [6.0] and r.s.stats()["duplicate_aliases"] == 1


# ----------------------------------------------------------------------------- modes, settings, the request

def test_keep_all_writes_everything_and_counts_what_strict_drops():
    strict, keep = Run(), Run(sequence_handling=KEEP_ALL)
    for r in (strict, keep):
        born(r)
        r.send("DDATA", 2, at=0.4)
        r.send("DDATA", 4, at=0.6)
        r.send("NDEATH", bdseq=9, at=0.7)
        r.tick(5.5)
    assert len(strict.rows) == 3 and len(keep.rows) == 5
    assert keep.s.stats() == {**strict.s.stats(), "sequence_handling": "keep_all"}
    assert keep.requests == strict.requests


def test_block_auto_rebirth_decides_but_sends_nothing():
    r = Run(block_auto_rebirth=True)
    r.send("NDATA", 0)
    s = r.s.stats()
    assert not r.requests and s["rebirth_requests"] == 1 and s["rebirth_requests_blocked"] == 1


def test_the_timeout_is_a_setting():
    r = Run(reorder_timeout_ms=2000)
    born(r)
    r.send("DDATA", 3, at=0.2)                                  # the DBIRTH at 0.0 was the last in order
    r.tick(2.0)
    r.tick(2.01)
    assert r.requested_at() == [2.01]


def test_held_messages_are_dropped_at_close():
    r = Run()
    born(r)
    r.send("DDATA", 5)
    r.s.close()
    assert r.s.stats()["dropped_shutdown"] == 1 and r.s.stats()["messages_held"] == 0


@pytest.mark.parametrize("settings", [{"sequence_handling": "drop"}, {"reorder_timeout_ms": 0}])
def test_invalid_settings(settings):
    with pytest.raises(ValueError):
        Sequencer(**settings)


def test_the_rebirth_request_bytes():
    # an NCMD as a Tahu-based host application sends it (captured): the Boolean datatype kept
    assert rebirth_payload(1792699282151).hex() == "08e7e5d0a9963412230a144e6f646520436f6e74726f6c2f5265626972746818e7e5d0a99634200b38007001"
    assert rebirth_topic("Plant1", "Line A") == "spBv1.0/Plant1/NCMD/Line A"


def test_edge_nodes_are_independent():
    r = Run()
    born(r)
    r.send("DDATA", 5, edge="Line B")                           # another edge node, never born: held there
    assert r.send("DDATA", 2, label="d2") == ["d2"]
    r.tick(5.5)
    assert r.requests == [(GROUP, "Line B", 5.5)]


def test_deviation_a_compressed_session_is_checked_decompressed():
    """Tahu's own checks read a compressed payload as received (only seq outside): a compressed NBIRTH would never
    pass and its edge node would be asked for a rebirth every 5 s."""
    import payloads

    r = Run()
    for kind, seq, bdseq in [("NBIRTH", 0, 0), ("DBIRTH", 1, None), ("DDATA", 2, None)]:
        p = pb.Payload(timestamp=TS, seq=seq)
        if bdseq is not None:
            p.metrics.add(name="bdSeq", datatype=4, long_value=bdseq)
        topic = f"spBv1.0/{GROUP}/{kind}/{EDGE}" + (f"/{DEV}" if kind[0] == "D" else "")
        r.rows += r.s.on_message(convert(topic, payloads.compressed(p.SerializeToString(), seq=seq), 0), 0.0)
    assert [row.message_type for row in r.rows] == ["NBIRTH", "DBIRTH", "DDATA"]
    r.tick(20.0)
    assert not r.requests and r.s.stats()["messages_dropped"] == 0


def test_keep_all_from_now_writes_the_held_messages_in_seq_order():
    r = Run()
    born(r)
    r.send("DDATA", 4, label="d4")
    r.send("DDATA", 3, label="d3")
    assert [_label(x) for x in r.s.keep_all_from_now()] == ["d3", "d4"]
    assert r.s.mode == KEEP_ALL and r.s.keep_all_from_now() == []
    assert r.send("DDATA", 9, label="d9") == ["d9"]              # every message from now on


# ----------------------------------------------------------------------------- the edge nodes' state

def node_state(r: Run, edge=EDGE):
    return next(n for n in r.s.edge_nodes() if n["edge_node_id"] == edge)


def test_births_bring_an_edge_node_and_its_devices_online_at_their_own_timestamps():
    r = Run()
    r.send("NBIRTH", 0, bdseq=3, ts=TS, at=1.0)
    r.send("DBIRTH", 1, ts=TS + 10)
    assert r.s.edge_nodes() == [{"group_id": GROUP, "edge_node_id": EDGE, "online": True, "since": TS,
                                 "changed_by": "NBIRTH", "bdseq": 3,
                                 "devices": [{"device_id": DEV, "online": True, "since": TS + 10, "changed_by": "DBIRTH"}]}]


def test_a_ddeath_sets_the_device_offline_at_the_ddeath_timestamp():
    r = Run()
    born(r)
    r.send("DDEATH", 2, ts=TS + 500, at=9.0)          # received at 9 s: the payload's time counts
    n = node_state(r)
    assert n["online"] and n["devices"] == [{"device_id": DEV, "online": False, "since": TS + 500, "changed_by": "DDEATH"}]


def test_an_ndeath_sets_the_edge_node_and_its_devices_offline_at_its_receipt():
    r = Run()
    born(r, bdseq=4)
    r.send("NDEATH", bdseq=4, ts=TS + 500, at=7.25)    # maybe the server's will: the host's time counts
    n = node_state(r)
    assert (n["online"], n["since"], n["changed_by"]) == (False, 7250, "NDEATH")
    assert n["devices"] == [{"device_id": DEV, "online": False, "since": 7250, "changed_by": "NDEATH"}]


def test_a_refused_death_changes_nothing():
    r = Run()
    born(r, bdseq=4)
    r.send("NDEATH", bdseq=5, at=3.0)                  # not the birth's bdSeq: dropped
    assert node_state(r)["online"] and node_state(r)["changed_by"] == "NBIRTH"


def test_the_reorder_timeout_sets_a_known_edge_node_offline_at_the_hosts_time():
    r = Run(clock=lambda: 1_800_000_000.5)
    born(r)
    r.send("NDATA", 3, at=1.0)                         # seq 2 missing
    r.tick(7.0)
    n = node_state(r)
    assert (n["online"], n["since"], n["changed_by"]) == (False, 1_800_000_000_500, "reorder timeout") and r.requests
    r.send("NBIRTH", 0, bdseq=1, ts=TS + 900, at=8.0)
    assert (node_state(r)["online"], node_state(r)["since"]) == (True, TS + 900)


def test_an_ndeath_keeps_the_time_of_a_device_a_ddeath_took_offline():
    r = Run()
    born(r, bdseq=4)
    r.send("DDEATH", 2, ts=TS + 500, at=5.0)
    r.send("NDEATH", bdseq=4, at=9.0)
    assert node_state(r)["devices"] == [{"device_id": DEV, "online": False, "since": TS + 500, "changed_by": "DDEATH"}]


def test_the_reorder_timeout_keeps_the_time_of_an_edge_node_already_offline():
    r = Run(clock=lambda: 1_800_000_000.0)
    born(r, bdseq=4)
    r.send("NDATA", 3, at=1.0)                         # held: seq 2 missing
    r.send("NDEATH", bdseq=4, at=2.0)                  # not held: the edge node offline at 2 s
    r.tick(7.0)                                        # the held message times out
    n = node_state(r)
    assert (n["online"], n["since"], n["changed_by"]) == (False, 2000, "NDEATH") and r.requests
