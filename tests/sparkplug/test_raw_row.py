"""raw_row: topic parsing, the skip rules and the sparkplug_raw row."""
from __future__ import annotations

import json
import uuid

import pytest

from conftest import FIXTURES
from unified_connector.sparkplug import to_tahu_json
from unified_connector.sparkplug.raw_row import (
    COLUMNS,
    OtherTopic,
    RawRow,
    Skip,
    SparkplugTopic,
    StateTopic,
    convert,
    parse_topic,
)

AT = 1790694245123     # 2026-09-29T15:04:05.123Z


def fixture(name: str) -> bytes:
    return (FIXTURES / "transmission" / name).read_bytes()


# ----------------------------------------------------------------------------- topics

def test_device_message():
    assert parse_topic("spBv1.0/Plant1/DBIRTH/Line A/M100") == SparkplugTopic("spBv1.0", "Plant1", "DBIRTH", "Line A", "M100")


def test_node_message_has_no_device():
    assert parse_topic("spBv1.0/Plant1/NDEATH/Line A") == SparkplugTopic("spBv1.0", "Plant1", "NDEATH", "Line A", None)


@pytest.mark.parametrize("message_type", ["NBIRTH", "NDEATH", "NDATA", "NCMD"])
def test_every_node_type(message_type):
    assert isinstance(parse_topic(f"spBv1.0/G/{message_type}/E"), SparkplugTopic)


@pytest.mark.parametrize("message_type", ["DBIRTH", "DDEATH", "DDATA", "DCMD"])
def test_every_device_type(message_type):
    assert isinstance(parse_topic(f"spBv1.0/G/{message_type}/E/D"), SparkplugTopic)


def test_state_topics():
    assert parse_topic("spBv1.0/STATE/databricks-zerobus") == StateTopic("databricks-zerobus", legacy=False)
    assert parse_topic("STATE/databricks-zerobus") == StateTopic("databricks-zerobus", legacy=True)


@pytest.mark.parametrize("topic", [
    "spBv1.0/G/DDATA/E",            # device message without a device
    "spBv1.0/G/NDATA/E/D",          # node message with a device
    "spBv1.0/G/XDATA/E",            # unknown type
    "spBv1.0/G/NDATA",              # too short
    "spBv1.0/G/DDATA/E/D/extra",    # too long
    "spBv1.0//NDATA/E",             # empty level
    "spAv1.0/G/NDATA/E",            # other namespace
    "plant/line1/temperature",      # plain MQTT
    "STATE/",
])
def test_everything_else(topic):
    assert isinstance(parse_topic(topic), OtherTopic)


# ----------------------------------------------------------------------------- rows

def test_device_message_row():
    payload = fixture("dbirth-1.bin")
    row = convert("spBv1.0/Plant1/DBIRTH/Line A/M100", payload, AT).row
    assert (row.group_id, row.edge_node_id, row.device_id, row.message_type) == ("Plant1", "Line A", "M100", "DBIRTH")
    assert row.namespace == "spBv1.0" and row.msg_topic == "spBv1.0/Plant1/DBIRTH/Line A/M100"
    assert row.msg == to_tahu_json(payload)                                      # the payload JSON, unchanged
    assert row.inserted_at == AT
    uuid.UUID(row.msg_id)


def test_node_message_has_null_device():
    row = convert("spBv1.0/Plant1/NDEATH/Line A", fixture("ndeath-1.bin"), AT).row
    assert row.device_id is None and row.message_type == "NDEATH"


def test_every_row_gets_its_own_id():
    p, t = fixture("ddata-1.bin"), "spBv1.0/Plant1/DDATA/Line A/M100"
    assert convert(t, p, AT).row.msg_id != convert(t, p, AT).row.msg_id


def test_state_and_foreign_topics_get_no_row():
    assert convert("spBv1.0/STATE/databricks-zerobus", b'{"online":true}', AT).row is None
    assert convert("STATE/databricks-zerobus", b"ONLINE", AT).row is None
    assert convert("factory/line1/temp", b"\x01", AT).row is None


def test_commands_get_no_row():
    # births, deaths and data are stored, NCMD/DCMD aren't
    p = fixture("ddata-1.bin")
    assert convert("spBv1.0/Plant1/NCMD/Line A", p, AT).row is None
    assert convert("spBv1.0/Plant1/DCMD/Line A/M100", p, AT).row is None
    assert convert("spBv1.0/Plant1/DCMD/Line A", p, AT).row is None        # malformed too
    for t in ("NBIRTH/Line A", "NDATA/Line A", "NDEATH/Line A", "DBIRTH/Line A/M100",
              "DDATA/Line A/M100", "DDEATH/Line A/M100"):
        assert convert(f"spBv1.0/Plant1/{t}", p, AT).row is not None, t


@pytest.mark.parametrize("topic", ["spBv1.0/Plant1/DDATA/Line A", "spBv1.0/Plant1/NDATA/Line A/M100",
                                   "spBv1.0/Plant1/FOO/Line A", "spBv1.0/Plant1//Line A", "spBv1.0/Plant1"])
def test_malformed_sparkplug_topic_gets_no_row(topic):
    # a malformed topic: nothing stored
    o = convert(topic, b"", AT)
    assert o.row is None and o.skip is Skip.MALFORMED_TOPIC and o.detail


def test_undecodable_payload_gets_no_row():
    # an undecodable payload: nothing stored
    o = convert("spBv1.0/Plant1/DDATA/Line A/M100", b"\xff\x01", AT)
    assert o.row is None and o.skip is Skip.UNDECODABLE_PAYLOAD and "2 bytes" in o.detail


def test_skip_reasons():
    assert convert("spBv1.0/STATE/databricks-zerobus", b"", AT).skip is Skip.STATE
    assert convert("factory/line1/temp", b"", AT).skip is Skip.FOREIGN_TOPIC
    assert convert("spBv1.0", b"", AT).skip is Skip.FOREIGN_TOPIC                  # no "spBv1.0/" prefix
    assert convert("spBv1.0/Plant1/NCMD/Line A", b"", AT).skip is Skip.COMMAND
    assert convert("spBv1.0/Plant1/NDATA/Line A", b"", AT).skip is None        # an empty payload decodes


def test_zerobus_record_has_the_sparkplug_raw_columns():
    row = RawRow("id-1", "spBv1.0/G/NDATA/E", "spBv1.0", "G", "NDATA", "E", None, '{"seq":1}', AT)
    record = row.to_zerobus_record()
    assert tuple(record) == COLUMNS == ("msg_id", "msg_topic", "namespace", "group_id", "message_type",
                                        "edge_node_id", "device_id", "msg", "inserted_at")
    assert record["inserted_at"] == "1790694245123"      # NUMBER(38,0) of epoch ms, as a string (Zerobus rejects a number)
    assert record["device_id"] is None
    assert record["msg"] == '{"seq":1}'                   # VARIANT as its JSON text (Zerobus rejects an object)
    assert json.loads(json.dumps(record)) == record
