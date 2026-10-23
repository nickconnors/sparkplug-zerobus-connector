"""sparkplug_connector.sparkplug.to_tahu_json against Java Eclipse Tahu (the JSON the connector writes).

Tests marked with the ``java_tahu`` fixture run Java Tahu on Java 17 through JPype and skip where it
isn't available; the rest compare with outputs saved from it (``expected/``) or with fixed values.
"""
from __future__ import annotations

import gzip
import json
import pathlib
import struct
import time
import zlib

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

import make_fixtures
import payloads
from conftest import EXPECTED, FIXTURES
from sparkplug_connector.sparkplug import TahuDecodeError, tahu_json, to_tahu_json
from sparkplug_connector.sparkplug.tahu_json import MetricTypes
from sparkplug_connector.sparkplug import sparkplug_b_decode_pb2 as dpb

TRANSMISSION = sorted((FIXTURES / "transmission").glob("*.bin"))
CASES = payloads.all_cases()


def _outcome(fn, raw: bytes) -> tuple[str, str]:
    try:
        return "json", fn(raw)
    except Exception as e:  # noqa: BLE001 - both sides must fail; the messages differ
        return "error", f"{type(e).__name__}: {e}"


def _assert_same(java: tuple[str, str], python: tuple[str, str]) -> None:
    if java[0] == python[0] == "error":
        return
    assert python == java


# ----------------------------------------------------------------------------- Transmission-shaped payloads (make_fixtures.py)

@pytest.mark.parametrize("fixture", TRANSMISSION, ids=lambda p: p.stem)
def test_transmission_payload_matches_saved_java_output(fixture: pathlib.Path):
    assert to_tahu_json(fixture.read_bytes()) == (EXPECTED / f"{fixture.stem}.json").read_text(encoding="utf-8")


@pytest.mark.parametrize("fixture", TRANSMISSION, ids=lambda p: p.stem)
def test_transmission_payload_matches_java(java_tahu, fixture: pathlib.Path):
    raw = fixture.read_bytes()
    assert to_tahu_json(raw) == java_tahu.to_tahu_json(raw)


def test_fixtures_are_what_make_fixtures_writes():
    written = {f.stem: f.read_bytes() for f in TRANSMISSION}
    assert written == {stem: raw for stem, (_topic, raw) in make_fixtures.fixtures().items()}


@pytest.mark.parametrize("compressed, plain", [("dbirth-gzip", "dbirth-1"), ("dbirth-deflate", "dbirth-1"),
                                               ("ddata-gzip", "ddata-1"), ("ddata-deflate", "ddata-2")])
def test_compressed_copies_decode_to_the_plain_payload(compressed: str, plain: str):
    assert (EXPECTED / f"{compressed}.json").read_text(encoding="utf-8") == (EXPECTED / f"{plain}.json").read_text(encoding="utf-8")


def test_resent_event_log_keeps_its_shape():
    """ddata-4/-5: an event log resent whole, as an edge that rebuilds a DataSet on a schedule sends it."""
    def log(stem: str) -> tuple[str, list]:
        raw = (EXPECTED / f"{stem}.json").read_text(encoding="utf-8")
        pump = next(m for m in json.loads(raw)["metrics"] if m["name"] == "Feed Pump")
        return raw, pump["value"]["metrics"][0]["value"]["rows"]
    (_, first), (raw, second) = log("ddata-4"), log("ddata-5")
    assert len(first) == len(second) == 51
    assert [r[0] for r in second[:-1]] == [r[0] for r in first[1:]]                # oldest gone, one new at the end
    assert first[-1][2] == "Unclassified" and second[-2][2] != "Unclassified"      # classified at the rebuild
    assert first[1][3] != int(first[1][3]) and f'[{second[0][0]},{second[0][1]},"{second[0][2]}",{int(second[0][3])}.0,' in raw
    assert all(r[3] == struct.unpack("<f", struct.pack("<f", r[3]))[0] for r in first + second)   # float precision


# ----------------------------------------------------------------------------- every data type, field and quirk

@pytest.mark.parametrize("name", list(CASES))
def test_case_matches_java(java_tahu, name: str):
    raw = CASES[name]
    _assert_same(_outcome(java_tahu.to_tahu_json, raw), _outcome(to_tahu_json, raw))


# ----------------------------------------------------------------------------- the births' types (a host's MetricDataTypeMap)

def _types(names: dict | None = None, aliases: dict | None = None) -> MetricTypes:
    t = MetricTypes()
    t.names.update(names or {})
    t.aliases.update(aliases or {})
    return t


UNTYPED_TYPES = _types(payloads.UNTYPED_NAMES, payloads.UNTYPED_ALIASES)


@pytest.mark.parametrize("types", [None, _types(), UNTYPED_TYPES], ids=["no types", "empty", "the births' types"])
@pytest.mark.parametrize("compress", [False, True], ids=["plain", "compressed"])
def test_untyped_metrics_match_java(java_tahu, types, compress):
    raw = payloads.untyped_metrics()
    if compress:
        raw = payloads.compressed(raw)
    java = _outcome(lambda b: java_tahu.to_tahu_json(b, types), raw)
    assert java[0] == "json"
    assert _outcome(lambda b: to_tahu_json(b, types), raw) == java


@pytest.mark.parametrize("name", list(CASES))
def test_case_matches_java_with_types(java_tahu, name: str):
    """Every case again with types for some of their names (types only change metrics sent without a datatype)."""
    types = _types({"unknown": 3, "UDT with an unknown-type member/unknown": 9, "member": 12}, {0: 3, 1: 9})
    raw = CASES[name]
    _assert_same(_outcome(lambda b: java_tahu.to_tahu_json(b, types), raw), _outcome(lambda b: to_tahu_json(b, types), raw))


def test_untyped_metrics_take_the_births_types():
    raw = payloads.untyped_metrics()
    plain = json.loads(to_tahu_json(raw))["metrics"]
    typed = json.loads(to_tahu_json(raw, UNTYPED_TYPES))["metrics"]
    assert plain.count(None) == 10                  # every untyped metric is null without types
    by_name = {m.get("name", f"alias {m.get('alias')}") if m else None: m for m in typed}
    assert by_name["Speed"]["dataType"] == "Float" and by_name["Speed"]["value"] == 1.5
    assert by_name["alias 7"] == {"alias": 7, "dataType": "Int32", "value": 5}
    assert by_name["Null typed"]["dataType"] == "String" and by_name["Null typed"]["value"] is None
    assert by_name["Weird null"]["dataType"] == "Int32" and by_name["Weird null"]["value"] is None
    assert [(m["name"], m["dataType"], m["value"]) for m in by_name["Motor"]["value"]["metrics"]] == [("rpm", "Int32", 3), ("temp", "Float", 20.5)]
    assert by_name["Pump"]["dataType"] == "Template" and by_name["Pump"]["value"]["metrics"][0]["value"] == 0.25
    assert by_name["Bool"]["value"] is True
    # left out: not in the types (also with a null value), a value the unknown datatype code can't give, a
    # template with such a member; still null: no name and no alias
    assert not {"Missing", "Missing null", "Weird", "Broken"} & set(by_name) and None in by_name


def test_untyped_tells_whether_types_could_change_the_payload():
    raw = payloads.untyped_metrics()
    assert tahu_json.untyped(tahu_json.decode(raw))
    assert tahu_json.untyped(tahu_json.decode(raw, UNTYPED_TYPES))          # no name and no alias stays null
    member_only = payloads.payload(payloads.metric("Motor", 19, template_value=dpb_template_with_untyped_member()))
    assert tahu_json.untyped(tahu_json.decode(member_only))
    assert not tahu_json.untyped(tahu_json.decode(member_only, _types({"Motor/rpm": 3})))
    assert not tahu_json.untyped(tahu_json.decode(payloads.int_metrics()))


def dpb_template_with_untyped_member():
    return payloads.pb.Payload.Template(template_ref="Motor", metrics=[payloads.metric("rpm", 0, int_value=3)])


def _metrics(raw: bytes) -> dict:
    return {m["name"] if m else None: m for m in json.loads(to_tahu_json(raw))["metrics"] if m is None or "name" in m}


def test_numbers_as_java_17_writes_them():
    m = _metrics(payloads.float_metrics())
    written = {name: json.dumps(v["value"]) for name, v in m.items()}
    raw = to_tahu_json(payloads.float_metrics())
    assert '"value":72.3}' in raw and '"value":0.1}' in raw and '"value":1.0E10}' in raw
    assert '"value":1.5E-5}' in raw and '"value":1.6777216E7}' in raw and '"value":-3.30283392E9}' in raw
    assert '"value":"NaN"}' in raw and '"value":"Infinity"}' in raw and '"value":"-Infinity"}' in raw
    assert '"value":1.4E-45}' in raw and '"value":4.9E-324}' in raw and '"value":1.66647859466125376E17}' in raw
    assert written["Double 2.3333332538604736"] == "2.3333332538604736"


def test_signed_integers():
    m = _metrics(payloads.int_metrics())
    assert m["Int8 -1"]["value"] == -1 and m["Int16 -300"]["value"] == -300 and m["Int32 -1073741054"]["value"] == -1073741054
    assert m[f"Int64 {payloads.LONG_MIN}"]["value"] == payloads.LONG_MIN
    assert m["UInt32 int"]["value"] == 0xFFFFFFFF and m[f"UInt64 {(1 << 64) - 1}"]["value"] == (1 << 64) - 1
    assert m["UInt8 40000"]["value"] == -25536            # Tahu: (short) cast of the int value
    assert m["DateTime -1"]["value"] == -1


def test_quirks_copied_from_tahu():
    arrays = _metrics(payloads.array_metrics())
    assert arrays["UInt8Array"]["value"] == [0, 127, -128, -56, -1]               # read as signed bytes
    assert "Int16Array odd bytes" not in arrays and "BooleanArray short" not in arrays  # metric left out
    assert arrays["StringArray"]["value"] == ["a", "", "bé", "bad �"]
    fields = json.loads(to_tahu_json(payloads.metric_field_metrics()))
    names = [m.get("name", "(alias only)") if m else None for m in fields["metrics"]]
    assert names.count(None) == 2                                                  # unknown data types -> null
    assert "UInt32 without value" not in names and "after the failures" in names
    assert fields["timestamp"] == -5 and fields["seq"] == -1                       # uint64 read as Java long
    datasets = _metrics(payloads.dataset_metrics())
    assert datasets["DataSet"]["value"]["rows"][0][11] is None                     # the string "null"
    assert datasets["DataSet"]["value"]["rows"][0][12] is None                     # DateTime Long.MIN_VALUE
    assert "DataSet mismatch" not in datasets and "DataSet unknown type, a row" not in datasets
    templates = _metrics(payloads.template_metrics())
    assert "UDT with a failing member" not in templates and "UDT with an unknown parameter type" not in templates
    assert templates["UDT with an unknown-type member"]["value"]["metrics"][1] is None
    parameters = {p["name"]: p for p in templates["UDT instance"]["value"]["parameters"]}
    assert "value" not in parameters["Empty"] and "value" not in parameters["Unset"]
    assert parameters["Zero"]["value"] == 0 and parameters["Boolean"]["value"] is False
    assert templates["UDT definition"]["value"]["reference"] == ""


def test_file_metric_over_6_kb_cannot_be_serialized():
    assert to_tahu_json(payloads.bytes_and_file_metrics(6144))
    with pytest.raises(TahuDecodeError, match="too long"):
        to_tahu_json(payloads.bytes_and_file_metrics(6145))


def test_truncated_deflate_raises_where_java_hangs():
    inner = payloads.int_metrics()
    whole = payloads.compressed(inner, "DEFLATE")
    p = dpb.Payload.FromString(whole)
    p.body = p.body[: len(p.body) // 2]
    with pytest.raises(TahuDecodeError, match="truncated"):
        to_tahu_json(p.SerializeToString())
    assert to_tahu_json(whole) == to_tahu_json(inner)


@pytest.mark.parametrize("algorithm", ["GZIP", "DEFLATE"])
def test_a_body_that_expands_beyond_the_limit_is_undecodable(algorithm, monkeypatch):
    inner = payloads.int_metrics()
    monkeypatch.setattr(tahu_json, "MAX_DECOMPRESSED_BYTES", len(inner))
    assert to_tahu_json(payloads.compressed(inner, algorithm)) == to_tahu_json(inner)   # exactly the limit
    monkeypatch.setattr(tahu_json, "MAX_DECOMPRESSED_BYTES", len(inner) - 1)
    with pytest.raises(TahuDecodeError, match="expands to more than"):
        to_tahu_json(payloads.compressed(inner, algorithm))


def test_a_decompression_bomb_is_refused_without_expanding_it():
    bomb = gzip.compress(b"\0" * (tahu_json.MAX_DECOMPRESSED_BYTES + 1))             # ~16 KB on the wire
    for body in (bomb, gzip.compress(b"\0" * (tahu_json.MAX_DECOMPRESSED_BYTES // 2)) * 3):   # one member, or several
        wrapped = payloads.payload(payloads.metric("algorithm", 12, string_value="GZIP"),
                                   uuid="SPBV1.0_COMPRESSED", body=body)
        with pytest.raises(TahuDecodeError, match="expands to more than"):
            to_tahu_json(wrapped)
    deflated = payloads.payload(uuid="SPBV1.0_COMPRESSED", body=zlib.compress(b"\0" * (tahu_json.MAX_DECOMPRESSED_BYTES + 1)))
    with pytest.raises(TahuDecodeError, match="expands to more than"):
        to_tahu_json(deflated)


def test_invalid_utf8_like_java():
    m = json.loads(to_tahu_json(payloads.invalid_utf8()))
    assert m["uuid"] == "u�" and m["metrics"][0]["name"] == "A�"
    assert m["metrics"][1]["name"] == "surrogate �"                          # one U+FFFD, not three


# ----------------------------------------------------------------------------- golden sample

def test_golden_dbirth_round_trips_to_the_sample():
    """A device birth as a table that sorts object keys stores it -> protobuf -> JSON: equal to the sample by value."""
    sample = payloads.load_json(FIXTURES / "sample_dbirth.json")
    assert json.loads(to_tahu_json(payloads.proto_from_tahu_json(sample))) == sample


def test_golden_dbirth_matches_java(java_tahu):
    raw = payloads.proto_from_tahu_json(payloads.load_json(FIXTURES / "sample_dbirth.json"))
    assert to_tahu_json(raw) == java_tahu.to_tahu_json(raw)


# ----------------------------------------------------------------------------- random payloads

def _s(max_size: int = 10) -> st.SearchStrategy[bytes]:
    """String field bytes: mostly valid UTF-8, sometimes not."""
    return st.one_of(st.text(max_size=max_size).map(lambda t: t.encode("utf-8", "surrogatepass")), st.binary(max_size=max_size))


# Java 17 spells a few numbers differently from 2^63 up and among the tiniest subnormals (java_format docstring)
_F32 = st.floats(width=32).filter(lambda x: x != x or x == 0 or 2.0 ** -140 <= abs(x) < 2.0 ** 63)
_F64 = st.floats().filter(lambda x: x != x or x == 0 or 2.0 ** -1060 <= abs(x) < 2.0 ** 63)
_U32 = st.integers(0, (1 << 32) - 1)
_U64 = st.integers(0, (1 << 64) - 1)


@st.composite
def _value_fields(draw, type_code: int, allow_unset: bool = True) -> dict:
    """protobuf value fields for a scalar of this type (sometimes the wrong field or none, like bad publishers)."""
    if allow_unset and draw(st.integers(0, 9)) == 0:
        return {}
    if type_code in (1, 2, 3, 5, 6, 7):
        return {"int_value": draw(_U32)} if type_code != 7 or draw(st.booleans()) else {"long_value": draw(_U64)}
    if type_code in (4, 8, 13):
        return {"long_value": draw(_U64)}
    if type_code == 9:
        return {"float_value": draw(_F32)}
    if type_code == 10:
        return {"double_value": draw(_F64)}
    if type_code == 11:
        return {"boolean_value": draw(st.booleans())}
    return {"string_value": draw(_s())}


@st.composite
def _properties(draw, depth: int = 0) -> dpb.Payload.PropertySet:
    ps = dpb.Payload.PropertySet()
    for _ in range(draw(st.integers(0, 5))):
        type_code = draw(st.sampled_from(list(range(0, 15)) + ([20, 21] if depth < 1 else [])))
        v = dpb.Payload.PropertyValue(type=type_code)
        if draw(st.integers(0, 6)) == 0:
            v.is_null = True
        elif type_code == 20:
            v.propertyset_value.CopyFrom(draw(_properties(depth + 1)))
        elif type_code == 21:
            v.propertysets_value.propertyset.extend(draw(st.lists(_properties(depth + 1), max_size=2)))
        else:
            for k, x in draw(_value_fields(type_code)).items():
                setattr(v, k, x)
        ps.keys.append(draw(st.sampled_from([b"Quality", b"executionMode", b"executionRate", b"engUnit"]) | _s(8)))
        ps.values.append(v)
    return ps


@st.composite
def _dataset(draw) -> dpb.Payload.DataSet:
    types = draw(st.lists(st.integers(1, 14), max_size=4))
    ds = dpb.Payload.DataSet(num_of_columns=len(types) if draw(st.integers(0, 9)) else draw(st.integers(0, 5)),
                             types=types, columns=[draw(_s(6)) for _ in types])
    for _ in range(draw(st.integers(0, 3))):
        row = ds.rows.add()
        for t in types:
            cell = row.elements.add()
            for k, x in draw(_value_fields(t)).items():
                setattr(cell, k, x)
    return ds


_ARRAY_PACK = {22: ("b", st.integers(-128, 127)), 23: ("h", st.integers(-32768, 32767)), 24: ("i", st.integers(-(1 << 31), (1 << 31) - 1)),
               25: ("q", st.integers(-(1 << 63), (1 << 63) - 1)), 26: ("B", st.integers(0, 255)), 27: ("H", st.integers(0, 65535)),
               28: ("I", _U32), 29: ("Q", _U64), 30: ("f", _F32), 31: ("d", _F64), 34: ("q", st.integers(-(1 << 63), (1 << 63) - 1))}


@st.composite
def _metric(draw, depth: int = 0) -> dpb.Payload.Metric:
    import struct
    type_code = draw(st.sampled_from(list(range(0, 20)) + list(range(22, 35)) if depth < 2 else list(range(1, 19))))
    m = dpb.Payload.Metric(datatype=type_code)
    if draw(st.integers(0, 9)):
        m.name = draw(_s(12))
    if draw(st.integers(0, 4)) == 0:
        m.alias = draw(_U64)
    if draw(st.integers(0, 5)):
        m.timestamp = draw(st.integers(0, 2 ** 45))
    for field in ("is_historical", "is_transient"):
        if draw(st.integers(0, 2)) == 0:
            setattr(m, field, draw(st.booleans()))
    if draw(st.integers(0, 5)) == 0:
        m.metadata.CopyFrom(dpb.Payload.MetaData(is_multi_part=draw(st.booleans()), file_name=draw(_s(6)), size=draw(_U64)))
    if draw(st.integers(0, 2)) == 0:
        m.properties.CopyFrom(draw(_properties()))
    if draw(st.integers(0, 12)) == 0:
        m.is_null = True
    elif type_code == 16:
        m.dataset_value.CopyFrom(draw(_dataset()))
    elif type_code == 19:
        t = m.template_value
        t.template_ref = draw(_s(8))
        t.is_definition = draw(st.booleans())
        t.metrics.extend(draw(st.lists(_metric(depth + 1), max_size=3)))
        for _ in range(draw(st.integers(0, 3))):
            ptype = draw(st.integers(0, 15))
            p = t.parameters.add(name=draw(_s(6)), type=ptype)
            for k, x in draw(_value_fields(ptype)).items():
                setattr(p, k, x)
    elif type_code in (17, 18):
        m.bytes_value = draw(st.binary(max_size=40))
    elif type_code in _ARRAY_PACK:
        fmt, values = _ARRAY_PACK[type_code]
        items = draw(st.lists(values, max_size=6))
        m.bytes_value = struct.pack(f"<{len(items)}{fmt}", *items) + (b"\x01" if draw(st.integers(0, 15)) == 0 else b"")
    elif type_code == 32:
        bits = draw(st.lists(st.booleans(), max_size=20))
        packed = bytes(sum(b << (7 - j) for j, b in enumerate(bits[i:i + 8])) for i in range(0, len(bits), 8))
        m.bytes_value = len(bits).to_bytes(4, "little") + packed
    elif type_code == 33:
        m.bytes_value = b"".join(draw(_s(6)).replace(b"\x00", b"") + b"\x00" for _ in range(draw(st.integers(0, 3))))
    else:
        for k, x in draw(_value_fields(type_code)).items():
            setattr(m, k, x)
    return m


@st.composite
def _payload(draw) -> bytes:
    p = dpb.Payload(metrics=draw(st.lists(_metric(), max_size=6)))
    if draw(st.integers(0, 5)):
        p.timestamp = draw(st.integers(0, 2 ** 45))
    if draw(st.integers(0, 5)):
        p.seq = draw(st.integers(0, 255))
    raw = p.SerializeToString()
    choice = draw(st.integers(0, 9))
    if choice == 0:
        return payloads.compressed(raw, "GZIP")
    if choice == 1:
        return payloads.compressed(raw, "DEFLATE")
    return raw


@settings(max_examples=500, deadline=None, suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
@given(raw=_payload())
def test_random_payloads_match_java(java_tahu, raw: bytes):
    java = _outcome(java_tahu.to_tahu_json, raw)
    assume("would loop forever" not in java[1])
    _assert_same(java, _outcome(to_tahu_json, raw))


# ----------------------------------------------------------------------------- speed

def test_speed_per_message(capsys):
    for name in ("ddata-1", "dbirth-1", "ddata-3"):
        raw = (FIXTURES / "transmission" / f"{name}.bin").read_bytes()
        runs = 200 if name != "ddata-3" else 20
        start = time.perf_counter()
        for _ in range(runs):
            to_tahu_json(raw)
        per_message_ms = (time.perf_counter() - start) / runs * 1000
        with capsys.disabled():
            print(f"\n  {name} ({len(raw)} bytes): {per_message_ms:.2f} ms per message")
        assert per_message_ms < 500
