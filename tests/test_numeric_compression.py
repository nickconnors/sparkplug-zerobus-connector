import pytest

from unified_connector.core.numeric_compression import (
    CompressionPolicy,
    CompressionRule,
    NumericCompressionMode,
    SdtState,
    resolve_effective_policy,
)


def _payload(v: float, t_ms: int):
    return {
        "value": v,
        "value_num": v,
        "event_time": t_ms * 1000,
        "topic_or_path": "plant1/line1/pump1/temperature/value",
    }


def test_sdt_first_point_emits():
    s = SdtState()
    out = s.offer(_payload(1.0, 1000), 1000, 1.0, deviation=0.1, max_interval_ms=0, min_interval_ms=0)
    assert out.emit_payload is not None
    assert out.emit_payload["value"] == 1.0
    assert out.forced_by_max_interval is False
    assert out.reset_due_to_out_of_order is False


def test_sdt_linear_within_band_filters():
    s = SdtState()
    assert s.offer(_payload(0.0, 0), 0, 0.0, deviation=1.0, max_interval_ms=0, min_interval_ms=0).emit_payload is not None
    assert s.offer(_payload(0.2, 1000), 1000, 0.2, deviation=1.0, max_interval_ms=0, min_interval_ms=0).emit_payload is None
    assert s.offer(_payload(0.4, 2000), 2000, 0.4, deviation=1.0, max_interval_ms=0, min_interval_ms=0).emit_payload is None


def test_sdt_closing_emits_previous_point():
    s = SdtState()
    assert s.offer(_payload(0.0, 0), 0, 0.0, deviation=0.1, max_interval_ms=0, min_interval_ms=0).emit_payload is not None
    assert s.offer(_payload(0.0, 1000), 1000, 0.0, deviation=0.1, max_interval_ms=0, min_interval_ms=0).emit_payload is None
    out = s.offer(_payload(10.0, 2000), 2000, 10.0, deviation=0.1, max_interval_ms=0, min_interval_ms=0)
    assert out.emit_payload is not None
    # PI-like semantics: emit previous point (t=1000), not current.
    assert out.emit_payload["event_time"] == 1000 * 1000
    assert out.emit_payload["value"] == 0.0


def test_sdt_max_interval_forces_emit():
    s = SdtState()
    assert s.offer(_payload(5.0, 0), 0, 5.0, deviation=1.0, max_interval_ms=1000, min_interval_ms=0).emit_payload is not None
    assert s.offer(_payload(5.0, 500), 500, 5.0, deviation=1.0, max_interval_ms=1000, min_interval_ms=0).emit_payload is None
    out = s.offer(_payload(5.0, 1000), 1000, 5.0, deviation=1.0, max_interval_ms=1000, min_interval_ms=0)
    assert out.emit_payload is not None
    assert out.forced_by_max_interval is True


def test_sdt_out_of_order_resets_and_emits_current():
    s = SdtState()
    assert s.offer(_payload(1.0, 1000), 1000, 1.0, deviation=0.1, max_interval_ms=0, min_interval_ms=0).emit_payload is not None
    out = s.offer(_payload(2.0, 900), 900, 2.0, deviation=0.1, max_interval_ms=0, min_interval_ms=0)
    assert out.emit_payload is not None
    assert out.emit_payload["event_time"] == 900 * 1000
    assert out.reset_due_to_out_of_order is True


def test_source_override_wins_over_global():
    global_policy = CompressionPolicy(mode=NumericCompressionMode.SDT, change_only=True, sdt_deviation=0.5)
    source_policy = CompressionPolicy(mode=NumericCompressionMode.DEADBAND, change_only=True, deadband=0.25)

    policy = resolve_effective_policy(
        global_policy=global_policy,
        global_rules=[],
        source_policy=source_policy,
        source_rules=[],
        tag_path="plant1/line1/pump1/temperature/value",
    )

    assert policy.mode == NumericCompressionMode.DEADBAND
    assert policy.deadband == pytest.approx(0.25)


def test_source_rule_has_highest_precedence():
    global_policy = CompressionPolicy(mode=NumericCompressionMode.SDT, change_only=True, sdt_deviation=0.5)
    global_rules = [CompressionRule(tag_path_regex=r".*temperature.*", mode=NumericCompressionMode.SDT, sdt_deviation=0.2)]
    source_policy = CompressionPolicy(mode=NumericCompressionMode.DEADBAND, change_only=True, deadband=0.25)
    source_rules = [CompressionRule(tag_path_regex=r".*temperature.*", mode=NumericCompressionMode.CHANGE_ONLY)]

    policy = resolve_effective_policy(
        global_policy=global_policy,
        global_rules=global_rules,
        source_policy=source_policy,
        source_rules=source_rules,
        tag_path="plant1/line1/pump1/temperature/value",
    )

    assert policy.mode == NumericCompressionMode.CHANGE_ONLY
