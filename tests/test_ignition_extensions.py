"""Ignition's extensions to Sparkplug B's types (``tahu_json``, marked ``Ignition``) against the JSON a Java decoder with
these extensions writes (``expected/ignition/<case>.json``, for the payloads of ``ignition_payloads.py``).

Byte for byte, except a Color cell (a ``java.awt.Color`` as Jackson writes it): Java orders its keys differently from
one JVM start to the next, so those cases compare by value."""
from __future__ import annotations

import json

import pytest

from conftest import EXPECTED
from ignition_payloads import CASES
from sparkplug_connector.sparkplug import TahuDecodeError, tahu_json, to_tahu_json

IGNITION = EXPECTED / "ignition"
COLOR_CASES = {"dataset-color", "dataset-color-only", "dataset-color-in-template", "alarm-reconciliation-color"}


def _outcome(raw: bytes) -> str:
    try:
        return to_tahu_json(raw)
    except TahuDecodeError as e:
        return json.dumps({"error": type(e).__name__})


@pytest.mark.parametrize("name", sorted(set(CASES) - COLOR_CASES))
def test_case_matches_the_java_decoder(name: str):
    assert _outcome(CASES[name]) == (IGNITION / f"{name}.json").read_text(encoding="utf-8")


@pytest.mark.parametrize("name", sorted(COLOR_CASES))
def test_color_case_matches_the_java_decoder_by_value(name: str):
    got, expected = _outcome(CASES[name]), (IGNITION / f"{name}.json").read_text(encoding="utf-8")
    assert json.loads(got) == json.loads(expected)
    assert len(got) == len(expected)                                    # only the order of keys differs


def test_a_color_cell():
    rows = _metric(CASES["dataset-color"], "Palette")["value"]["rows"]
    red, half, none = (row[1] for row in rows)
    assert {k: v for k, v in red.items() if k != "colorSpace"} == {
        "red": 255, "green": 0, "blue": 0, "alpha": 255, "transparency": 1, "rgb": -65536}
    assert (half["red"], half["green"], half["blue"], half["alpha"]) == (0x12, 0x34, 0x56, 255)   # alpha bits not read
    assert none is None
    assert red["colorSpace"]["cs_sRGB"] is True and len(red["colorSpace"]["profile"]["data"]) > 9000
    assert _metric(CASES["dataset-color"], "Palette")["value"]["types"] == ["String", "Color"]


def test_every_case_has_its_expected_json():
    assert {p.stem for p in IGNITION.glob("*.json")} == set(CASES)


def _metric(raw: bytes, name: str) -> dict | None:
    return next((m for m in json.loads(to_tahu_json(raw))["metrics"] if m and m.get("name") == name), None)


def test_the_extension_property_types_keep_their_metric_and_string_value():
    m = _metric(CASES["property-all-three"], "Motor/Speed")
    assert m["value"] == 72.5
    assert m["properties"]["doc"] == {"type": "Document", "value": "{}"}
    assert m["properties"]["arr"] == {"type": "Array", "value": "[]"}
    assert m["properties"]["ds"] == {"type": "DataSet", "value": ""}


def test_a_failing_property_is_left_out_not_its_metric():
    m = _metric(CASES["property-unknown-type"], "Motor/Speed")
    assert m["value"] == 72.5 and list(m["properties"]) == ["engUnit"]


def test_a_null_property_value_is_left_out():
    assert _metric(CASES["property-document-null"], "Motor/Speed")["properties"]["doc"] == {"type": "Document"}


def test_alarm_metrics():
    alarm = _metric(CASES["alarm-other-datatype"], "Alarms/HighTemp")
    assert alarm["dataType"] == "String" and alarm["value"] == '{"state":"Cleared"}'
    reconcile = _metric(CASES["alarm-reconciliation-other-datatype"], "Alarms/_reconcile")
    assert reconcile["dataType"] == "DataSet" and reconcile["value"]["rows"] == [["id-2"]]


@pytest.mark.parametrize("name", ["property-document", "property-unknown-type", "alarm-other-datatype",
                                  "alarm-reconciliation-other-datatype", "property-document-null", "dataset-color"])
def test_plain_tahu_decodes_without_the_extensions(name: str):
    """Within ``plain_tahu()`` the payloads decode as Tahu alone decodes them (another JSON for each of these)."""
    with tahu_json.plain_tahu():
        plain = _outcome(CASES[name])
    assert plain != _outcome(CASES[name])
    with tahu_json.plain_tahu():
        metrics = json.loads(plain)["metrics"]
    if name in ("property-document", "property-unknown-type", "dataset-color"):
        assert [m["name"] for m in metrics] == ["Motor/Run"]          # Tahu drops the metric with the property
    elif name == "alarm-other-datatype":
        assert metrics[0]["dataType"] == "Int32"
    elif name == "property-document-null":
        assert metrics[0]["properties"]["doc"] == {"type": "Unknown", "value": None}
