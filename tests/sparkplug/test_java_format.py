"""java_format: Java 17's number spelling, Java's UTF-8 decoding, Jackson's escaping and HashMap order.

Fixed values were measured on Java 17.0.17; the ``java_tahu`` tests compare with the JVM directly.
"""
from __future__ import annotations

import math
import random
import struct

import pytest

from unified_connector.sparkplug.java_format import (
    java_double_to_string,
    java_float_to_string,
    java_hashmap_order,
    java_utf8_decode,
    json_double,
    json_float,
    json_string,
)


def f32(x: float) -> float:
    return struct.unpack("<f", struct.pack("<f", x))[0]


@pytest.mark.parametrize("value, java", [
    (72.3, "72.3"), (0.1, "0.1"), (2.0, "2.0"), (-0.0, "-0.0"), (1e10, "1.0E10"), (1.5e-5, "1.5E-5"),
    (16777217.0, "1.6777216E7"), (-3302833920.0, "-3.30283392E9"), (1.401298464324817e-45, "1.4E-45"),
    (1e-3, "0.001"), (9999999.0, "9999999.0"), (3.4028234663852886e38, "3.4028235E38"),
    (2.0 ** -126, "1.17549435E-38"),
])
def test_float_to_string(value, java):
    assert java_float_to_string(f32(value)) == java


@pytest.mark.parametrize("value, java", [
    (0.1, "0.1"), (2.0, "2.0"), (1e7, "1.0E7"), (1e-4, "1.0E-4"), (123456.789, "123456.789"), (5e-324, "4.9E-324"),
    (166647859466125376.0, "1.66647859466125376E17"), (1.7976931348623157e308, "1.7976931348623157E308"),
])
def test_double_to_string(value, java):
    assert java_double_to_string(value) == java


def test_jdk19_spelling_is_available():
    assert java_float_to_string(f32(-3302833920.0), jdk17=False) == "-3.302834E9"


def test_non_finite_numbers_are_json_strings():
    assert json_float(math.nan) == '"NaN"' and json_double(math.inf) == '"Infinity"' and json_double(-math.inf) == '"-Infinity"'


def test_json_string_escapes_like_jackson():
    assert json_string('a"b\\c/d') == '"a\\"b\\\\c/d"'
    assert json_string("\x00\x08\x09\x0a\x0c\x0d\x1f\x7f") == '"\\u0000\\b\\t\\n\\f\\r\\u001F\x7f"'
    assert json_string("é漢😀 ") == '"é漢😀 "'


@pytest.mark.parametrize("raw, text", [
    (b"ok \xe2\x82\xac", "ok €"), (b"A\xff", "A�"), (b"\xed\xa0\x80", "�"), (b"\xc0\xaf", "��"),
    (b"\xf4\x90\x41", "��A"), (b"\xf0\x9f\x98", "�"), (b"\xe2\x82", "�"),
])
def test_utf8_decode_like_java(raw, text):
    assert java_utf8_decode(raw) == text


@pytest.mark.parametrize("keys, java", [
    (["Quality", "executionMode"], ["executionMode", "Quality"]),
    (["Quality", "executionMode", "executionRate"], ["executionMode", "executionRate", "Quality"]),
    (["executionMode", "executionRate"], ["executionRate", "executionMode"]),
])
def test_hashmap_order_seen_in_transmission_births(keys, java):
    assert [k for k, _ in java_hashmap_order([(k, None) for k in keys])] == java


def test_hashmap_repeated_key_keeps_first_position_last_value():
    assert java_hashmap_order([("b", 1), ("a", 2), ("b", 3)]) == [("a", 2), ("b", 3)]


# ----------------------------------------------------------------------------- against the JVM

def test_numbers_match_java_17(java_tahu):
    import jpype
    jfloat, jdouble = jpype.JClass("java.lang.Float"), jpype.JClass("java.lang.Double")
    rng = random.Random(1)
    for _ in range(20000):
        f = f32(10 ** rng.uniform(-40, math.log10(2 ** 63)) * rng.choice((1, -1)))
        assert java_float_to_string(f) == str(jfloat.toString(jfloat.intBitsToFloat(struct.unpack("<i", struct.pack("<f", f))[0])))
        d = 10 ** rng.uniform(-320, math.log10(2 ** 63)) * rng.choice((1, -1))
        assert java_double_to_string(d) == str(jdouble.toString(d))


def test_powers_of_two_match_java_17(java_tahu):
    import jpype
    jfloat, jdouble = jpype.JClass("java.lang.Float"), jpype.JClass("java.lang.Double")
    for e in range(-149, 63):
        f = 2.0 ** e
        assert java_float_to_string(f) == str(jfloat.toString(jfloat.intBitsToFloat(struct.unpack("<i", struct.pack("<f", f))[0]))), e
    for e in range(-1074, 63):
        if e == -1073:
            continue                                    # documented remainder: Java 17 "1.0E-323", ours "9.9E-324"
        assert java_double_to_string(2.0 ** e) == str(jdouble.toString(2.0 ** e)), e


def test_utf8_matches_java(java_tahu):
    import jpype
    jstring, utf8 = jpype.JClass("java.lang.String"), jpype.JClass("java.nio.charset.StandardCharsets").UTF_8
    jbytes = jpype.JArray(jpype.JByte)
    rng = random.Random(2)
    pool = [0x00, 0x41, 0x7F, 0x80, 0x9F, 0xA0, 0xBF, 0xC0, 0xC2, 0xDF, 0xE0, 0xED, 0xEF, 0xF0, 0xF4, 0xF5, 0xFF]
    for _ in range(20000):
        raw = bytes(rng.choice(pool) if rng.random() < 0.8 else rng.randrange(256) for _ in range(rng.randint(1, 8)))
        assert java_utf8_decode(raw) == str(jstring(jbytes(raw), utf8))


def test_hashmap_order_matches_java(java_tahu):
    import jpype
    hashmap = jpype.JClass("java.util.HashMap")
    rng = random.Random(3)
    for _ in range(2000):
        keys = ["".join(rng.choices("abcdefghijkQEMR_", k=rng.randint(1, 10))) for _ in range(rng.randint(1, 30))]
        a = hashmap()
        for k in keys:
            a.put(k, 0)
        b = hashmap()
        b.putAll(a)
        assert [k for k, _ in java_hashmap_order([(k, 0) for k in keys])] == [str(k) for k in b.keySet()]
