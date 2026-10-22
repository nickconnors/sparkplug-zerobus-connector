"""Write fixtures/transmission/: synthetic Sparkplug B payloads shaped like the ones MQTT Transmission publishes.

Ignition with MQTT Transmission (UDTs published as Sparkplug templates, "Convert UDTs" off)
sends these shapes; the names, UDTs and values here are made up:

- NBIRTH of edge node ``Line A`` (group ``Plant1``): the UDT definitions (``Motor``; ``Pump`` with a nested
  ``Motor`` and a DataSet member), bdSeq and Transmission's node metrics (``Node Control/Rebirth``, ...);
- DBIRTH of device ``M100``: two ``Motor`` instances and a ``Pump``, plain tags incl. arrays, members with
  bad quality (no value and a ``Quality`` property), ``Device Control/Rebirth``;
- DDATA: one template metric per changed member, and a large store-and-forward flush (``isHistorical``);
- an event log the edge rebuilds on a schedule and resends whole (``Feed Pump/Events``, about 50 rows), twice,
  15 minutes apart: the oldest row gone, a new one unclassified, the previous newest reclassified, and Double
  cells with float precision, one of them whole (``34.0``), as an Ignition DataSet of minutes looks;
- GZIP and DEFLATE copies of a DBIRTH and a DDATA in Transmission's ``SPBV1.0_COMPRESSED`` wrapper; NDEATH.

``golden_dbirth()`` is a second device's birth for ``fixtures/sample_dbirth.json`` (``make_expected.py``).
Deterministic (fixed timestamps, seeded values). After a change, regenerate Java Tahu's JSON too:

    .venv/bin/python tests/make_fixtures.py
    TAHU_JVM=<Java 17 libjvm> .venv/bin/python tests/make_expected.py
"""
from __future__ import annotations

import gzip
import hashlib
import pathlib
import random
import struct
import sys
import zlib
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
from payloads import u32, u64  # noqa: E402
from unified_connector.sparkplug import sparkplug_b_pb2 as pb  # noqa: E402

OUT = HERE / "fixtures" / "transmission"
GROUP, EDGE_NODE, DEVICE, GOLDEN_DEVICE = "Plant1", "Line A", "M100", "M200"
T0 = 1767268800000                  # 2026-01-01T12:00:00Z
BD_SEQ = 7

INT8, INT16, INT32, INT64, FLOAT, DOUBLE, BOOLEAN, STRING, DATETIME, DATASET, TEMPLATE = 1, 2, 3, 4, 9, 10, 11, 12, 13, 16, 19
INT32_ARRAY, FLOAT_ARRAY, BOOLEAN_ARRAY, STRING_ARRAY = 24, 30, 32, 33

# Ignition quality codes as Transmission sends them in the ``Quality`` property (Int32)
BAD_NOT_CONNECTED, BAD_STALE, ERROR_CONFIGURATION, UNCERTAIN_LAST_KNOWN = 0x80000207, 0x8000020A, 0xC0000102, 0x40000100

# (name, datatype, executionMode, executionRate): None = a memory tag (no properties; the definition holds its value)
MOTOR_MEMBERS = (
    ("Speed", FLOAT, 1, None), ("Current", FLOAT, 1, None), ("Winding Temp", DOUBLE, 1, 5000),
    ("Energized", BOOLEAN, 1, None), ("Starts", INT32, 1, None), ("Run Hours", INT64, 1, 10000),
    ("Fault Code", INT16, 1, None), ("Direction", INT8, 1, None), ("Mode", STRING, 2, None),
    ("Last Start", DATETIME, 1, None), ("Serial Number", STRING, None, None),
)
MOTOR_PARAMETERS = (("Location", STRING), ("Rated kW", FLOAT), ("Poles", INT32))
PUMP_MEMBERS = (
    ("Flow", DOUBLE, 1, None), ("Discharge Pressure", FLOAT, 1, None), ("Valve Open", BOOLEAN, 2, None),
    ("Events", DATASET, 1, 5000), ("Drive", TEMPLATE, None, None),
)
PUMP_PARAMETERS = (("Location", STRING), ("Max Flow", DOUBLE))
EVENT_COLUMNS = (("Time", DATETIME), ("Code", INT32), ("Text", STRING), ("Duration", DOUBLE), ("Acknowledged", BOOLEAN), ("Occurrences", INT64))
DEFINITION_DEFAULTS = {"Serial Number": "unknown"}


# ----------------------------------------------------------------------------- values

def set_value(target: Any, datatype: int, value: Any) -> None:
    """A metric's, parameter's or DataSet cell's value, the way Tahu's Java encoder writes it."""
    if datatype in (INT8, INT16, INT32):
        target.int_value = u32(value)
    elif datatype in (INT64, DATETIME):
        target.long_value = u64(value)
    elif datatype == FLOAT:
        target.float_value = value
    elif datatype == DOUBLE:
        target.double_value = value
    elif datatype == BOOLEAN:
        target.boolean_value = value
    elif datatype == STRING:
        target.string_value = value
    elif datatype == DATASET:
        target.dataset_value.CopyFrom(value)
    elif datatype == INT32_ARRAY:
        target.bytes_value = struct.pack(f"<{len(value)}i", *value)
    elif datatype == FLOAT_ARRAY:
        target.bytes_value = struct.pack(f"<{len(value)}f", *value)
    elif datatype == BOOLEAN_ARRAY:
        bits = bytes(sum(b << (7 - j) for j, b in enumerate(value[i:i + 8])) for i in range(0, len(value), 8))
        target.bytes_value = len(value).to_bytes(4, "little") + bits
    elif datatype == STRING_ARRAY:
        target.bytes_value = b"".join(s.encode("utf-8") + b"\x00" for s in value)
    else:
        raise ValueError(f"no value encoding for datatype {datatype}")


def properties(mode: int | None, rate: int | None, quality: int | None = None) -> pb.Payload.PropertySet:
    ps = pb.Payload.PropertySet()
    for key, type_code, field, value in (("Quality", INT32, "int_value", quality), ("executionMode", INT32, "int_value", mode),
                                         ("executionRate", INT64, "long_value", rate)):
        if value is not None:
            ps.keys.append(key)
            ps.values.append(pb.Payload.PropertyValue(type=type_code, **{field: value}))
    return ps


def events(rng: random.Random, rows: int, ts: int) -> pb.Payload.DataSet:
    ds = pb.Payload.DataSet(num_of_columns=len(EVENT_COLUMNS), columns=[c for c, _ in EVENT_COLUMNS],
                            types=[t for _, t in EVENT_COLUMNS])
    for i in range(rows):
        row = ds.rows.add()
        cells = (ts - (rows - i) * 61_000, rng.choice((101, 205, 310)), rng.choice(("Overload", "Dry run", "Low suction")),
                 round(rng.uniform(0.5, 90.0), 3), i % 2 == 0, rng.randrange(1, 1 << 40))
        for (_, datatype), value in zip(EVENT_COLUMNS, cells):
            set_value(row.elements.add(), datatype, value)
    return ds


def f32(x: float) -> float:
    """A Double cell that came from a float: float precision, as Ignition writes computed minutes."""
    return struct.unpack("<f", struct.pack("<f", x))[0]


LOG_REASONS, UNCLASSIFIED = ((101, "Overload"), (205, "Dry run"), (310, "Low suction")), (0, "Unclassified")


def event_log(seed: int, end: int, rows: int) -> list[list[Any]]:
    """Rows of an event log in ``EVENT_COLUMNS`` order: ``rows`` events (about 14 minutes apart) that ended before
    ``end``, oldest first, whole seconds; the newest still unclassified."""
    rng = random.Random(seed)
    out, at = [], end // 1000 * 1000 - 60_000
    for _ in range(rows):
        seconds = rng.randrange(10, 600)
        code, text = rng.choice(LOG_REASONS)
        at -= seconds * 1000
        out.append([at, code, text, f32(seconds / 60), True, rng.randrange(1, 1 << 40)])
        at -= rng.randrange(120, 900) * 1000
    out.reverse()
    out[-1][1:3], out[-1][4] = UNCLASSIFIED, False
    return out


def dataset(rows: list[list[Any]]) -> pb.Payload.DataSet:
    ds = pb.Payload.DataSet(num_of_columns=len(EVENT_COLUMNS), columns=[c for c, _ in EVENT_COLUMNS],
                            types=[t for _, t in EVENT_COLUMNS])
    for cells in rows:
        row = ds.rows.add()
        for (_, datatype), value in zip(EVENT_COLUMNS, cells):
            set_value(row.elements.add(), datatype, value)
    return ds


def instance_values(rng: random.Random, kind: str, ts: int) -> dict[str, Any]:
    """Live values of one UDT instance at a DBIRTH."""
    if kind == "Motor":
        return {"Speed": round(rng.uniform(1400, 1500), 1), "Current": round(rng.uniform(10, 40), 2),
                "Winding Temp": rng.uniform(40, 80), "Energized": True, "Starts": rng.randrange(100, 5000),
                "Run Hours": rng.randrange(1000, 90000), "Fault Code": -1, "Direction": rng.choice((-1, 1)),
                "Mode": "Auto", "Last Start": ts - rng.randrange(3_600_000, 86_400_000), "Serial Number": f"SN-{rng.randrange(10000, 99999)}"}
    return {"Flow": rng.uniform(20, 120), "Discharge Pressure": round(rng.uniform(2, 9), 2), "Valve Open": True,
            "Events": events(rng, 3, ts)}


# ----------------------------------------------------------------------------- metrics

def metric(name: str, datatype: int, ts: int, value: Any = None, **fields: Any) -> pb.Payload.Metric:
    m = pb.Payload.Metric(name=name, timestamp=ts, datatype=datatype, **fields)
    if value is None:
        m.is_null = True
    else:
        set_value(m, datatype, value)
    return m


def node_control(name: str, datatype: int, ts: int, value: Any, documentation: str | None) -> pb.Payload.Metric:
    m = metric(name, datatype, ts, value, is_null=False)
    if documentation:
        m.properties.keys.append("documentation")
        m.properties.values.append(pb.Payload.PropertyValue(type=STRING, string_value=documentation))
    return m


def udt(kind: str, ts: int, member_ts: int, definition: bool, name: str | None = None, values: dict | None = None,
        parameters: dict | None = None, quality: dict | None = None, rng: random.Random | None = None) -> pb.Payload.Metric:
    """A UDT definition (NBIRTH) or instance (DBIRTH) as a template metric, members in UDT order."""
    members, params = (MOTOR_MEMBERS, MOTOR_PARAMETERS) if kind == "Motor" else (PUMP_MEMBERS, PUMP_PARAMETERS)
    t = pb.Payload.Template(is_definition=definition)
    if not definition:
        t.template_ref = kind
    for member_name, datatype, mode, rate in members:
        if datatype == TEMPLATE:                       # the nested Motor: a reference to its definition, or an instance
            if definition:
                nested = udt("Motor", member_ts, member_ts, True, member_name)
                nested.template_value.template_ref = "Motor"
                nested.template_value.is_definition = False
                nested.template_value.ClearField("parameters")
            else:
                nested = udt("Motor", member_ts, member_ts, False, member_name, instance_values(rng, "Motor", ts),
                             {"Location": parameters["Location"] + "/Drive", "Rated kW": 7.5, "Poles": 4})
            nested.ClearField("metadata")
            nested.ClearField("properties")
            t.metrics.append(nested)
            continue
        bad = (quality or {}).get(member_name)
        value = DEFINITION_DEFAULTS.get(member_name) if definition else (None if bad else values[member_name])
        m = metric(member_name, datatype, member_ts, value)
        if not definition:
            m.is_historical = m.is_transient = False
        m.metadata.SetInParent()
        m.properties.CopyFrom(properties(mode, rate, bad))
        m.properties.SetInParent()
        t.metrics.append(m)
    for param_name, datatype in params:
        p = t.parameters.add(name=param_name, type=datatype)
        set_value(p, datatype, (parameters or {}).get(param_name, "" if datatype == STRING else 0))
    m = pb.Payload.Metric(name=name or kind, timestamp=ts, datatype=TEMPLATE, template_value=t)
    m.metadata.SetInParent()
    if definition:
        m.metadata.md5 = hashlib.md5(t.SerializeToString()).hexdigest()
    m.properties.SetInParent()
    return m


def change(instance: str, kind: str, member: str, datatype: int, ts: int, value: Any, historical: bool = False,
           nested: tuple[str, str] | None = None) -> pb.Payload.Metric:
    """A DDATA metric: the instance as a template carrying the one member that changed."""
    inner = metric(member, datatype, ts, value, is_null=False)
    if nested:                                         # a member of the nested UDT: one more template level
        inner = pb.Payload.Metric(name=nested[0], timestamp=ts, datatype=TEMPLATE, is_null=False,
                                  template_value=pb.Payload.Template(template_ref=nested[1], is_definition=False, metrics=[inner]))
    m = pb.Payload.Metric(name=instance, timestamp=ts, datatype=TEMPLATE, is_null=False,
                          template_value=pb.Payload.Template(template_ref=kind, is_definition=False, metrics=[inner]))
    if historical:
        m.is_historical = True
    return m


def payload(ts: int, metrics: list[pb.Payload.Metric], seq: int | None) -> bytes:
    p = pb.Payload(timestamp=ts, metrics=metrics)
    if seq is not None:
        p.seq = seq
    return p.SerializeToString()


def compressed(inner: bytes, algorithm: str) -> bytes:
    """Transmission's compression: the whole payload as the body of an ``SPBV1.0_COMPRESSED`` payload."""
    p = pb.Payload.FromString(inner)
    body = gzip.compress(inner, mtime=0) if algorithm == "GZIP" else zlib.compress(inner)
    wrapper = pb.Payload(timestamp=p.timestamp + 1, seq=p.seq, uuid="SPBV1.0_COMPRESSED", body=body)
    wrapper.metrics.append(metric("algorithm", STRING, p.timestamp + 1, algorithm, is_null=False))
    return wrapper.SerializeToString()


# ----------------------------------------------------------------------------- messages

def nbirth() -> bytes:
    ts, defined = T0 + 5_000, T0 - 3_600_000
    return payload(ts, [
        udt("Motor", ts - 1, defined, True),
        node_control("bdSeq", INT64, ts, BD_SEQ, None),
        node_control("Node Control/Next Server", BOOLEAN, ts, False, "Writeable tag to request the Edge Node to walk to the next MQTT server"),
        udt("Pump", ts - 1, defined, True),
        node_control("Node Info/Transmission Version", STRING, ts, "5.0.4", "The version of MQTT Transmission installed at the Edge Node"),
        node_control("Node Control/Rebirth", BOOLEAN, ts, False, "Writeable tag to request the Edge Node to resend its cached NBIRTH "
                     "and DBIRTH messages without disconnecting or sending DEATH messages first"),
    ], seq=0)


def dbirth(device: str = DEVICE, seed: int = 1, ts: int = T0 + 5_018) -> bytes:
    rng = random.Random(seed)
    member_ts = ts - 1_063
    location = f"{GROUP}/{EDGE_NODE}/{device}"
    plain = [
        metric("Status/Ambient Temp", DOUBLE, member_ts, rng.uniform(15, 30), is_historical=False, is_transient=False),
        metric("Status/Zone Temps", FLOAT_ARRAY, member_ts, [round(rng.uniform(20, 90), 1) for _ in range(4)]),
        metric("Status/Interlocks", BOOLEAN_ARRAY, member_ts, [True, True, False, True, True, True, True, False, True, False]),
        metric("Status/Cycle Counts", INT32_ARRAY, member_ts, [rng.randrange(0, 1 << 20) for _ in range(5)] + [-1]),
        metric("Status/Recipe Steps", STRING_ARRAY, member_ts, ["Fill", "Heat", "Hold", "Drain"]),
    ]
    for m in plain:
        m.metadata.SetInParent()
        m.properties.SetInParent()
    return payload(ts, [
        udt("Motor", ts - 19, member_ts, False, "Motor 1", instance_values(rng, "Motor", ts),
            {"Location": location + "/Motor 1", "Rated kW": 15.0, "Poles": 4}),
        udt("Pump", ts - 19, member_ts, False, "Feed Pump", instance_values(rng, "Pump", ts),
            {"Location": location + "/Feed Pump", "Max Flow": 150.0}, {"Valve Open": UNCERTAIN_LAST_KNOWN}, rng=rng),
        *plain,
        node_control("Device Control/Rebirth", BOOLEAN, ts, False, None),
        udt("Motor", ts - 19, member_ts, False, "Motor 2", instance_values(rng, "Motor", ts),
            {"Location": location + "/Motor 2", "Rated kW": 22.0, "Poles": 2},
            {"Winding Temp": BAD_NOT_CONNECTED, "Last Start": BAD_STALE, "Mode": ERROR_CONFIGURATION}),
    ], seq=1)


def ddata(seed: int, ts: int, ticks: int, seq: int, historical: bool = False) -> bytes:
    """Changes once a second: speeds and currents mostly, now and then a pump, a nested drive, an event or a mode."""
    rng = random.Random(seed)
    metrics = []
    start = ts - ticks * 1_000 - 26
    for tick in range(ticks):
        at = start + tick * 1_000
        metrics.append(change("Motor 1", "Motor", "Speed", FLOAT, at, round(rng.uniform(1400, 1500), 1), historical))
        metrics.append(change("Motor 2", "Motor", "Current", FLOAT, at, round(rng.uniform(10, 40), 2), historical))
        if tick % 2 == 0:
            metrics.append(change("Feed Pump", "Pump", "Flow", DOUBLE, at, rng.uniform(20, 120), historical))
        if tick % 5 == 1:
            metrics.append(change("Feed Pump", "Pump", "Speed", FLOAT, at, round(rng.uniform(900, 1000), 1), historical,
                                  nested=("Drive", "Motor")))
        if tick % 40 == 7:
            metrics.append(change("Motor 2", "Motor", "Starts", INT32, at, rng.randrange(100, 5000), historical))
            metrics.append(change("Motor 2", "Motor", "Mode", STRING, at, rng.choice(("Auto", "Manual")), historical))
            metrics.append(change("Motor 2", "Motor", "Energized", BOOLEAN, at, rng.random() < 0.5, historical))
        if tick % 70 == 30:
            metrics.append(change("Feed Pump", "Pump", "Events", DATASET, at, events(rng, 2, at), historical))
            metrics.append(change("Motor 1", "Motor", "Run Hours", INT64, at, rng.randrange(1000, 90000), historical))
    return payload(ts, metrics, seq)


def resent_logs(ts: int) -> tuple[bytes, bytes]:
    """The event log sent whole at ``ts`` (with a speed change) and again 15 minutes later, changed as a rebuild changes it."""
    first = event_log(seed=21, end=ts, rows=51)
    second = [list(r) for r in first[1:]]
    second[0][3] = float(int(second[0][3]))          # started before the cutoff now: whole minutes, e.g. 34.0
    second[-1][1:3], second[-1][4] = LOG_REASONS[1], True   # an operator classified the previous newest
    second.append([ts // 1000 * 1000 + 400_000, *UNCLASSIFIED, f32(330 / 60), False, 4242])
    at, later = ts - 1_027, ts + 900_000
    return (payload(ts, [change("Motor 1", "Motor", "Speed", FLOAT, at, 1452.5),
                         change("Feed Pump", "Pump", "Events", DATASET, at, dataset(first))], seq=5),
            payload(later, [change("Feed Pump", "Pump", "Events", DATASET, later - 1_027, dataset(second))], seq=6))


def ndeath() -> bytes:
    ts = T0 - 700_000
    return payload(ts, [node_control("bdSeq", INT64, ts, BD_SEQ, None)], seq=None)


def golden_dbirth() -> bytes:
    """A second device's birth: the golden sample (``fixtures/sample_dbirth.json``) is Java Tahu's JSON of it."""
    return dbirth(GOLDEN_DEVICE, seed=2, ts=T0 + 5_031)


def fixtures() -> dict[str, tuple[str, bytes]]:
    """file stem -> (MQTT topic, payload)."""
    node, device = f"spBv1.0/{GROUP}/{{}}/{EDGE_NODE}", f"spBv1.0/{GROUP}/{{}}/{EDGE_NODE}/{DEVICE}"
    birth = dbirth()
    ddata_1 = ddata(seed=11, ts=T0 + 9_620, ticks=3, seq=3)
    ddata_2 = ddata(seed=12, ts=T0 + 11_640, ticks=3, seq=4)
    log_1, log_2 = resent_logs(T0 + 13_660)
    return {
        "ndeath-1": (node.format("NDEATH"), ndeath()),
        "nbirth-1": (node.format("NBIRTH"), nbirth()),
        "dbirth-1": (device.format("DBIRTH"), birth),
        "ddata-3": (device.format("DDATA"), ddata(seed=13, ts=T0 + 5_240, ticks=150, seq=2, historical=True)),
        "ddata-1": (device.format("DDATA"), ddata_1),
        "ddata-2": (device.format("DDATA"), ddata_2),
        "ddata-4": (device.format("DDATA"), log_1),
        "ddata-5": (device.format("DDATA"), log_2),
        "dbirth-gzip": (device.format("DBIRTH"), compressed(birth, "GZIP")),
        "dbirth-deflate": (device.format("DBIRTH"), compressed(birth, "DEFLATE")),
        "ddata-gzip": (device.format("DDATA"), compressed(ddata_1, "GZIP")),
        "ddata-deflate": (device.format("DDATA"), compressed(ddata_2, "DEFLATE")),
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    lines = ["# file\tMQTT topic (synthetic payloads shaped like MQTT Transmission's, written by make_fixtures.py)"]
    for stem, (topic, raw) in fixtures().items():
        (OUT / f"{stem}.bin").write_bytes(raw)
        lines.append(f"{stem}.bin\t{topic}" + ("\t(compression GZIP)" if stem.endswith("gzip") else
                                              "\t(compression DEFLATE)" if stem.endswith("deflate") else ""))
        print(f"wrote {stem}.bin ({len(raw)} bytes)")
    (OUT / "topics.tsv").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
