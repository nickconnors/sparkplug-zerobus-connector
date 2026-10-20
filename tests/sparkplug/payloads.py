"""Sparkplug B payloads for the tests: builders, one case per data type and quirk, and the golden sample's protobuf.

Values are encoded the way Tahu's Java encoder does: signed integers as two's complement in the
unsigned protobuf fields. ``bytes_view`` builders use the decode-only classes (string fields as
bytes), so payloads can carry invalid UTF-8.
"""
from __future__ import annotations

import gzip
import json
import math
import struct
import zlib
from typing import Any

from unified_connector.sparkplug import sparkplug_b_decode_pb2 as dpb
from unified_connector.sparkplug import sparkplug_b_pb2 as pb

TS = 1790696925611
LONG_MIN, LONG_MAX = -(1 << 63), (1 << 63) - 1


def u32(v: int) -> int:
    return v & 0xFFFFFFFF


def u64(v: int) -> int:
    return v & 0xFFFFFFFFFFFFFFFF


def metric(name: str | None, datatype: int, **fields: Any) -> pb.Payload.Metric:
    m = pb.Payload.Metric(datatype=datatype, **fields)
    if name is not None:
        m.name = name
    return m


def payload(*metrics: pb.Payload.Metric, timestamp: int | None = TS, seq: int | None = 1, **fields: Any) -> bytes:
    p = pb.Payload(metrics=list(metrics), **fields)
    if timestamp is not None:
        p.timestamp = timestamp
    if seq is not None:
        p.seq = seq
    return p.SerializeToString()


def compressed(inner: bytes, algorithm: str | None = "GZIP", seq: int = 1) -> bytes:
    """A payload wrapped the way MQTT Transmission compresses it."""
    body = gzip.compress(inner) if algorithm == "GZIP" else zlib.compress(inner)
    metrics = [] if algorithm is None else [metric("algorithm", 12, timestamp=TS, string_value=algorithm)]
    return payload(*metrics, timestamp=TS, seq=seq, uuid="SPBV1.0_COMPRESSED", body=body)


def props(*entries: tuple[str, pb.Payload.PropertyValue]) -> pb.Payload.PropertySet:
    return pb.Payload.PropertySet(keys=[k for k, _ in entries], values=[v for _, v in entries])


def pv(type_code: int, **fields: Any) -> pb.Payload.PropertyValue:
    return pb.Payload.PropertyValue(type=type_code, **fields)


# ----------------------------------------------------------------------------- one case per data type

def int_metrics() -> bytes:
    return payload(
        *[metric(f"Int8 {v}", 1, timestamp=TS, int_value=u32(v)) for v in (-128, -1, 0, 127)],
        *[metric(f"Int16 {v}", 2, timestamp=TS, int_value=u32(v)) for v in (-32768, -300, 32767)],
        *[metric(f"Int32 {v}", 3, timestamp=TS, int_value=u32(v)) for v in (-(1 << 31), -1073741054, -1, (1 << 31) - 1)],
        *[metric(f"Int64 {v}", 4, timestamp=TS, long_value=u64(v)) for v in (LONG_MIN, -1, LONG_MAX)],
        *[metric(f"UInt8 {v}", 5, timestamp=TS, int_value=v) for v in (0, 200, 255, 40000)],
        *[metric(f"UInt16 {v}", 6, timestamp=TS, int_value=v) for v in (0, 65535)],
        metric("UInt32 int", 7, timestamp=TS, int_value=0xFFFFFFFF),
        metric("UInt32 long", 7, timestamp=TS, long_value=1 << 40),
        *[metric(f"UInt64 {v}", 8, timestamp=TS, long_value=v) for v in (0, LONG_MAX + 1, (1 << 64) - 1)],
        metric("DateTime -1", 13, timestamp=TS, long_value=u64(-1)),
        metric("DateTime max", 13, timestamp=TS, long_value=LONG_MAX),
        metric("Boolean", 11, timestamp=TS, boolean_value=True),
        metric("Boolean unset", 11, timestamp=TS),
        metric("Int32 unset", 3, timestamp=TS),
    )


FLOATS = (72.3, 0.1, 89.5, 2.0, -0.0, 1e10, 1.5e-5, 16777217.0, -3302833920.0, 3.4028234663852886e38,
          1.401298464324817e-45, 1e-3, 9999999.0, math.nan, math.inf, -math.inf)
DOUBLES = (0.1, 2.0, -0.0, 1e7, 1e-4, 123456.789, 1e-3, 5e-324, 1.7976931348623157e308, 166647859466125376.0,
           2.3333332538604736, math.nan, math.inf, -math.inf)


def float_metrics() -> bytes:
    return payload(
        *[metric(f"Float {v!r}", 9, timestamp=TS, float_value=v) for v in FLOATS],
        *[metric(f"Double {v!r}", 10, timestamp=TS, double_value=v) for v in DOUBLES],
        metric("Float unset", 9, timestamp=TS),
    )


STRINGS = ("", "plain", 'quote " backslash \\ slash /', "control \x00\x01\x08\x09\x0a\x0c\x0d\x1f\x7f",
           "non-ASCII é ü 漢字 😀", "  ", "null")


def string_metrics() -> bytes:
    return payload(
        *[metric(f"String {i}", 12, timestamp=TS, string_value=s) for i, s in enumerate(STRINGS)],
        metric("Text", 14, timestamp=TS, string_value="text"),
        metric("UUID", 15, timestamp=TS, string_value="0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"),
        metric("String unset", 12, timestamp=TS),
        metric("name with \" and \\", 12, timestamp=TS, string_value="x"),
    )


def bytes_and_file_metrics(file_size: int = 300) -> bytes:
    return payload(
        metric("Bytes empty", 17, timestamp=TS, bytes_value=b""),
        metric("Bytes all", 17, timestamp=TS, bytes_value=bytes(range(256))),
        metric("File", 18, timestamp=TS, bytes_value=bytes(i % 256 for i in range(file_size)),
               metadata=pb.Payload.MetaData(file_name="f.bin", file_type="bin", size=file_size, md5="0" * 32)),
    )


def array_metrics() -> bytes:
    def packed(fmt: str, values) -> bytes:
        return struct.pack(f"<{len(values)}{fmt}", *values)
    booleans = [True, False, True, True, False, False, True, False, True, True]
    return payload(
        metric("Int8Array", 22, timestamp=TS, bytes_value=packed("b", [-128, -1, 0, 127])),
        metric("Int16Array", 23, timestamp=TS, bytes_value=packed("h", [-32768, -1, 32767])),
        metric("Int32Array", 24, timestamp=TS, bytes_value=packed("i", [-(1 << 31), -1, (1 << 31) - 1])),
        metric("Int64Array", 25, timestamp=TS, bytes_value=packed("q", [LONG_MIN, -1, LONG_MAX])),
        metric("UInt8Array", 26, timestamp=TS, bytes_value=packed("B", [0, 127, 128, 200, 255])),
        metric("UInt16Array", 27, timestamp=TS, bytes_value=packed("H", [0, 65535])),
        metric("UInt32Array", 28, timestamp=TS, bytes_value=packed("I", [0, 0xFFFFFFFF])),
        metric("UInt64Array", 29, timestamp=TS, bytes_value=packed("Q", [0, (1 << 64) - 1])),
        metric("FloatArray", 30, timestamp=TS, bytes_value=packed("f", [72.3, -0.0, 1e10, math.nan, math.inf])),
        metric("DoubleArray", 31, timestamp=TS, bytes_value=packed("d", [0.1, 1e7, 166647859466125376.0, -math.inf])),
        metric("BooleanArray", 32, timestamp=TS, bytes_value=len(booleans).to_bytes(4, "little") + bytes([0b10110010, 0b11000000])),
        metric("StringArray", 33, timestamp=TS, bytes_value=b"a\x00\x00b\xc3\xa9\x00bad \xff\x00tail without terminator"),
        metric("DateTimeArray", 34, timestamp=TS, bytes_value=packed("q", [0, TS, -1])),
        metric("Int16Array odd bytes", 23, timestamp=TS, bytes_value=b"\x01\x02\x03"),
        metric("BooleanArray short", 32, timestamp=TS, bytes_value=(20).to_bytes(4, "little") + b"\xff"),
        metric("Int32Array empty", 24, timestamp=TS, bytes_value=b""),
    )


def dataset_metrics() -> bytes:
    cell = pb.Payload.DataSet.DataSetValue
    types = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
    full = pb.Payload.DataSet.Row(elements=[
        cell(int_value=u32(-1)), cell(int_value=u32(-300)), cell(int_value=u32(-1073741054)), cell(long_value=u64(-1)),
        cell(int_value=255), cell(int_value=65535), cell(int_value=0xFFFFFFFF), cell(long_value=(1 << 64) - 1),
        cell(float_value=72.3), cell(double_value=2.3333332538604736), cell(boolean_value=True),
        cell(string_value="null"), cell(long_value=u64(LONG_MIN)), cell(string_value="text")])
    empty = pb.Payload.DataSet.Row(elements=[cell() for _ in types])
    ds = pb.Payload.DataSet(num_of_columns=len(types), columns=[f"c{t}" for t in types], types=types, rows=[full, empty])
    mismatch = pb.Payload.DataSet(num_of_columns=2, columns=["a"], types=[12, 12])
    unknown_type_no_rows = pb.Payload.DataSet(num_of_columns=1, columns=["a"], types=[15])
    unknown_type_with_row = pb.Payload.DataSet(num_of_columns=1, columns=["a"], types=[15],
                                               rows=[pb.Payload.DataSet.Row(elements=[cell(string_value="x")])])
    return payload(
        metric("DataSet", 16, timestamp=TS, dataset_value=ds),
        metric("DataSet empty", 16, timestamp=TS, dataset_value=pb.Payload.DataSet()),
        metric("DataSet unset", 16, timestamp=TS),
        metric("DataSet mismatch", 16, timestamp=TS, dataset_value=mismatch),
        metric("DataSet unknown type, no rows", 16, timestamp=TS, dataset_value=unknown_type_no_rows),
        metric("DataSet unknown type, a row", 16, timestamp=TS, dataset_value=unknown_type_with_row),
    )


def template_metrics() -> bytes:
    param = pb.Payload.Template.Parameter
    parameters = [param(name="Int8", type=1, int_value=u32(-1)), param(name="Int32", type=3, int_value=u32(-5)),
                  param(name="Int64", type=4, long_value=u64(-1)), param(name="UInt32", type=7, int_value=0xFFFFFFFF),
                  param(name="UInt64", type=8, long_value=(1 << 64) - 1), param(name="Float", type=9, float_value=72.3),
                  param(name="Double", type=10, double_value=0.1), param(name="Boolean", type=11, boolean_value=False),
                  param(name="Zero", type=3, int_value=0), param(name="String", type=12, string_value="Site A"),
                  param(name="Empty", type=12, string_value=""), param(name="Unset", type=12),
                  param(name="DateTime", type=13, long_value=TS), param(name="Text", type=14, string_value="t")]
    member = metric("member", 9, timestamp=TS, float_value=1.5, is_historical=True, is_transient=False)
    nested = metric("nested", 19, timestamp=TS, template_value=pb.Payload.Template(template_ref="Inner", metrics=[member]))
    definition = pb.Payload.Template(version="1", is_definition=True, metrics=[metric("m", 3, int_value=0)],
                                     parameters=[param(name="Site", type=12)])
    failing_member = pb.Payload.Template(template_ref="Bad", metrics=[member, metric("no uint32 value", 7, timestamp=TS)])
    unknown_member = pb.Payload.Template(template_ref="Unknown member", metrics=[member, metric("unknown", 0, timestamp=TS)])
    bad_parameter = pb.Payload.Template(template_ref="Bad parameter", parameters=[param(name="UUID", type=15, string_value="u")])
    return payload(
        metric("UDT instance", 19, timestamp=TS,
               template_value=pb.Payload.Template(template_ref="Outer", metrics=[member, nested], parameters=parameters)),
        metric("UDT definition", 19, timestamp=TS, template_value=definition),
        metric("Template unset", 19, timestamp=TS),
        metric("UDT with a failing member", 19, timestamp=TS, template_value=failing_member),
        metric("UDT with an unknown-type member", 19, timestamp=TS, template_value=unknown_member),
        metric("UDT with an unknown parameter type", 19, timestamp=TS, template_value=bad_parameter),
    )


def property_metrics() -> bytes:
    inner = props(("engUnit", pv(12, string_value="degC")), ("engHigh", pv(10, double_value=100.0)))
    every = props(
        ("Quality", pv(3, int_value=u32(-1073741054))), ("executionMode", pv(3, int_value=1)), ("executionRate", pv(4, long_value=5000)),
        ("Int8", pv(1, int_value=u32(-1))), ("Int16", pv(2, int_value=u32(-2))), ("UInt8", pv(5, int_value=200)),
        ("UInt16", pv(6, int_value=65535)), ("UInt32 int", pv(7, int_value=0xFFFFFFFF)), ("UInt32 long", pv(7, long_value=1 << 33)),
        ("UInt64", pv(8, long_value=(1 << 64) - 1)), ("Float", pv(9, float_value=0.1)), ("Double", pv(10, double_value=0.1)),
        ("Boolean", pv(11, boolean_value=True)), ("String", pv(12, string_value="s")), ("DateTime", pv(13, long_value=TS)),
        ("Text", pv(14, string_value="t")), ("null", pv(3, is_null=True)), ("unknown type, null", pv(0, is_null=True)),
        ("PropertySet", pv(20, propertyset_value=inner)),
        ("PropertySetList", pv(21, propertysets_value=pb.Payload.PropertySetList(propertyset=[inner, props()]))))
    duplicate = props(("a", pv(3, int_value=1)), ("b", pv(3, int_value=2)), ("a", pv(3, int_value=3)))
    many = props(*[(f"key{i}", pv(3, int_value=i)) for i in range(30)])
    return payload(
        metric("all property types", 11, timestamp=TS, boolean_value=False, properties=every),
        metric("duplicate key", 11, timestamp=TS, boolean_value=False, properties=duplicate),
        metric("30 keys", 11, timestamp=TS, boolean_value=False, properties=many),
        metric("empty properties", 11, timestamp=TS, boolean_value=False, properties=props()),
        metric("unknown property type", 11, timestamp=TS, boolean_value=False, properties=props(("x", pv(0, int_value=1)))),
        metric("fewer values than keys", 11, timestamp=TS, boolean_value=False,
               properties=pb.Payload.PropertySet(keys=["a", "b"], values=[pv(3, int_value=1)])),
    )


def metric_field_metrics() -> bytes:
    return payload(
        metric("alias", 3, alias=7, timestamp=TS, int_value=1),
        metric(None, 3, alias=8, timestamp=TS, int_value=2),
        metric("alias above 2^63", 3, alias=(1 << 64) - 1, int_value=3),
        metric("no timestamp", 3, int_value=4),
        metric("flags", 3, timestamp=TS, is_historical=True, is_transient=True, int_value=5),
        metric("null value", 3, timestamp=TS, is_null=True, int_value=6),
        metric("metadata", 3, timestamp=TS, int_value=7, metadata=pb.Payload.MetaData(is_multi_part=True, content_type="text/plain", size=12, seq=3)),
        metric("empty metadata", 3, timestamp=TS, int_value=8, metadata=pb.Payload.MetaData()),
        metric("unknown datatype", 0, timestamp=TS, int_value=9),
        metric("PropertySet datatype", 20, timestamp=TS, int_value=10),
        metric("UInt32 without value", 7, timestamp=TS),
        metric("after the failures", 3, timestamp=TS, int_value=11),
        timestamp=u64(-5), seq=(1 << 64) - 1,
    )


def payload_field_cases() -> dict[str, bytes]:
    return {
        "empty": b"",
        "no timestamp, no seq": payload(metric("a", 3, int_value=1), timestamp=None, seq=None),
        "uuid and body, not compressed": payload(metric("a", 3, int_value=1), uuid="my-uuid", body=b"\x00\x01binary"),
        "no metrics": payload(),
    }


def compression_cases() -> dict[str, bytes]:
    inner = int_metrics()
    return {
        "GZIP": compressed(inner, "GZIP"),
        "DEFLATE": compressed(inner, "DEFLATE"),
        "no algorithm metric (DEFLATE)": compressed(inner, None),
        "GZIP, two members": payload(metric("algorithm", 12, string_value="GZIP"), uuid="SPBV1.0_COMPRESSED",
                                     body=gzip.compress(inner) + gzip.compress(b"")),
        "unknown algorithm": payload(metric("algorithm", 12, string_value="BROTLI"), uuid="SPBV1.0_COMPRESSED", body=b"x"),
        "algorithm lower case": payload(metric("algorithm", 12, string_value="gzip"), uuid="SPBV1.0_COMPRESSED", body=gzip.compress(inner)),
        "no body": payload(metric("algorithm", 12, string_value="GZIP"), uuid="SPBV1.0_COMPRESSED"),
        "corrupt GZIP": payload(metric("algorithm", 12, string_value="GZIP"), uuid="SPBV1.0_COMPRESSED", body=b"\x1f\x8bnot gzip"),
        "corrupt DEFLATE": payload(metric("algorithm", 12, string_value="DEFLATE"), uuid="SPBV1.0_COMPRESSED", body=b"\x78\x9c\xff\xff"),
        "nameless metric": payload(metric(None, 12, alias=1, string_value="GZIP"), uuid="SPBV1.0_COMPRESSED", body=gzip.compress(inner)),
    }


def invalid_utf8() -> bytes:
    """String fields with invalid UTF-8 (Java replaces it with U+FFFD)."""
    p = dpb.Payload(timestamp=TS, seq=1, uuid=b"u\xff")
    p.metrics.add(name=b"A\xff", datatype=12, timestamp=TS, string_value=b"x\xc3")
    p.metrics.add(name=b"surrogate \xed\xa0\x80", datatype=12, string_value=b"\xf0\x9f\x98")
    p.metrics.add(name=b"overlong \xc0\xaf", datatype=12, string_value=b"\xe2\x82\xac ok")
    return p.SerializeToString()


def all_cases() -> dict[str, bytes]:
    cases = {
        "integers": int_metrics(), "floats": float_metrics(), "strings": string_metrics(),
        "bytes and file": bytes_and_file_metrics(), "file 6144 bytes": bytes_and_file_metrics(6144),
        "file 6145 bytes": bytes_and_file_metrics(6145), "arrays": array_metrics(), "datasets": dataset_metrics(),
        "templates": template_metrics(), "properties": property_metrics(), "metric fields": metric_field_metrics(),
        "invalid UTF-8": invalid_utf8(), "not protobuf": b"\xff\xff\xff",
    }
    cases.update({f"payload: {k}": v for k, v in payload_field_cases().items()})
    cases.update({f"compressed: {k}": v for k, v in compression_cases().items()})
    return cases


# ----------------------------------------------------------------------------- golden sample

def proto_from_tahu_json(doc: dict[str, Any]) -> bytes:
    """The protobuf a Tahu-shaped JSON payload came from (the golden sample's JSON back to protobuf)."""
    return _payload_from_json(doc).SerializeToString()


_TYPE_CODES = {name: code for code, name in {**{i: n for i, n in enumerate(
    ["Unknown", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float", "Double", "Boolean",
     "String", "DateTime", "Text", "UUID", "DataSet", "Bytes", "File", "Template", "PropertySet", "PropertySetList",
     "Int8Array", "Int16Array", "Int32Array", "Int64Array", "UInt8Array", "UInt16Array", "UInt32Array", "UInt64Array",
     "FloatArray", "DoubleArray", "BooleanArray", "StringArray", "DateTimeArray"])}}.items()}
# Tahu's JSON reads UInt8Array as signed bytes and the wider unsigned arrays as Java's signed types
_ARRAY_FORMATS = {"Int8Array": "b", "Int16Array": "h", "Int32Array": "i", "Int64Array": "q", "UInt8Array": "b",
                  "UInt16Array": "H", "UInt32Array": "I", "UInt64Array": "Q", "FloatArray": "f", "DoubleArray": "d",
                  "DateTimeArray": "q"}


def _set_value(target: Any, type_name: str, value: Any, strings: tuple[str, ...]) -> None:
    if type_name in ("Int8", "Int16", "Int32", "UInt8", "UInt16"):
        target.int_value = u32(int(value or 0))
    elif type_name in ("Int64", "UInt32", "UInt64", "DateTime"):
        target.long_value = u64(int(value or 0))
    elif type_name == "Float":
        target.float_value = float(value or 0)
    elif type_name == "Double":
        target.double_value = float(value or 0)
    elif type_name == "Boolean":
        target.boolean_value = bool(value)
    elif type_name in strings:
        target.string_value = "null" if value is None else str(value)
    elif type_name in _ARRAY_FORMATS:
        target.bytes_value = struct.pack(f"<{len(value)}{_ARRAY_FORMATS[type_name]}", *value)
    elif type_name == "BooleanArray":
        bits = bytes(sum(b << (7 - j) for j, b in enumerate(value[i:i + 8])) for i in range(0, len(value), 8))
        target.bytes_value = len(value).to_bytes(4, "little") + bits
    elif type_name == "StringArray":
        target.bytes_value = b"".join(v.encode("utf-8") + b"\x00" for v in value)
    else:
        raise ValueError(f"unsupported type in test fixture: {type_name}")


def _payload_from_json(doc: dict[str, Any]) -> pb.Payload:
    p = pb.Payload()
    if "timestamp" in doc:
        p.timestamp = u64(doc["timestamp"])
    if "seq" in doc:
        p.seq = u64(doc["seq"])
    if "uuid" in doc:
        p.uuid = doc["uuid"]
    p.metrics.extend(_metric_from_json(m) for m in doc.get("metrics", []))
    return p


def _metric_from_json(doc: dict[str, Any]) -> pb.Payload.Metric:
    m = pb.Payload.Metric()
    if "name" in doc:
        m.name = doc["name"]
    if "alias" in doc:
        m.alias = u64(doc["alias"])
    if "timestamp" in doc:
        m.timestamp = u64(doc["timestamp"])
    type_name = doc["dataType"]
    m.datatype = _TYPE_CODES[type_name]
    if "isHistorical" in doc:
        m.is_historical = doc["isHistorical"]
    if "isTransient" in doc:
        m.is_transient = doc["isTransient"]
    if "metaData" in doc:
        md = doc["metaData"]
        m.metadata.CopyFrom(pb.Payload.MetaData(
            is_multi_part=md.get("multiPart", False), content_type=md.get("contentType", ""), size=u64(md.get("size", 0)),
            seq=u64(md.get("seq", 0)), file_name=md.get("fileName", ""), file_type=md.get("fileType", ""),
            md5=md.get("md5", ""), description=md.get("description", "")))
    if "properties" in doc:
        m.properties.CopyFrom(_properties_from_json(doc["properties"]))
    value = doc.get("value")
    if value is None:
        m.is_null = True
    elif type_name == "DataSet":
        m.dataset_value.CopyFrom(_dataset_from_json(value))
    elif type_name == "Template":
        m.template_value.CopyFrom(_template_from_json(value))
    else:
        _set_value(m, type_name, value, ("String", "Text", "UUID"))
    return m


def _properties_from_json(doc: dict[str, Any]) -> pb.Payload.PropertySet:
    ps = pb.Payload.PropertySet()
    for key, prop in doc.items():
        v = pb.Payload.PropertyValue(type=_TYPE_CODES[prop["type"]])
        if prop.get("value") is None:
            v.is_null = True
        else:
            _set_value(v, prop["type"], prop["value"], ("String", "Text"))
        ps.keys.append(key)
        ps.values.append(v)
    return ps


def _template_from_json(doc: dict[str, Any]) -> pb.Payload.Template:
    t = pb.Payload.Template()
    if "version" in doc:
        t.version = doc["version"]
    if "reference" in doc:
        t.template_ref = doc["reference"]
    if "isDefinition" in doc:
        t.is_definition = doc["isDefinition"]
    t.metrics.extend(_metric_from_json(m) for m in doc.get("metrics", []))
    for prm in doc.get("parameters", []):
        p = pb.Payload.Template.Parameter(name=prm["name"], type=_TYPE_CODES[prm["type"]])
        _set_value(p, prm["type"], prm.get("value"), tuple(_TYPE_CODES))
        t.parameters.append(p)
    return t


def _dataset_from_json(doc: dict[str, Any]) -> pb.Payload.DataSet:
    ds = pb.Payload.DataSet(num_of_columns=u64(doc["numberOfColumns"]), columns=doc["columnNames"],
                            types=[_TYPE_CODES[t] for t in doc["types"]])
    for row in doc["rows"]:
        r = ds.rows.add()
        for type_name, value in zip(doc["types"], row):
            _set_value(r.elements.add(), type_name, value, tuple(_TYPE_CODES))
    return ds


def load_json(path) -> Any:
    with open(path, encoding="utf-8") as f:
        return json.load(f)
