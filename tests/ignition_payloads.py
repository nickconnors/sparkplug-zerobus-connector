"""Payloads with Ignition's extensions to Sparkplug B's types, one case per extension and edge case.

Ignition's MQTT Transmission publishes types that Sparkplug B (and Eclipse Tahu) don't define:

- property types DataSet (16), Array (-10001) and Document (-10002), each carried as the property's string value;
- the DataSet column type Color (-10000), a cell's ``int_value`` being the colour's RGB;
- metrics whose ``metaData.contentType`` is ``Alarm`` (the value is the metric's string) or ``AlarmReconciliation``
  (the value is the metric's DataSet).

``CASES``: case name -> payload bytes. ``expected/ignition/<case>.json`` holds the JSON a Java decoder with these
extensions writes for it (or, for a payload it can't decode, ``{"error": "<exception class>"}``).
"""
from __future__ import annotations

from payloads import TS, metric, payload, u32
from sparkplug_connector.sparkplug import sparkplug_b_pb2 as pb

COLOR, ARRAY, DOCUMENT = -10000, -10001, -10002
DATASET_PROPERTY = 16


def _props(*entries: tuple[str, pb.Payload.PropertyValue]) -> pb.Payload.PropertySet:
    ps = pb.Payload.PropertySet()
    for key, value in entries:
        ps.keys.append(key)
        ps.values.append(value)
    return ps


def _pv(type_code: int, **fields) -> pb.Payload.PropertyValue:
    return pb.Payload.PropertyValue(type=u32(type_code), **fields)


def _float_with(*entries: tuple[str, pb.Payload.PropertyValue], name: str = "Motor/Speed") -> pb.Payload.Metric:
    return metric(name, 9, timestamp=TS, float_value=72.5, properties=_props(*entries))


def _run() -> pb.Payload.Metric:
    """A plain metric after the one under test: it must stay in the payload whatever happens to the other."""
    return metric("Motor/Run", 11, timestamp=TS, boolean_value=True)


def _dataset(types: list[int], rows: list[list[dict]]) -> pb.Payload.DataSet:
    ds = pb.Payload.DataSet(num_of_columns=len(types), columns=[f"c{i}" for i in range(len(types))],
                            types=[u32(t) for t in types])
    for row in rows:
        r = ds.rows.add()
        for cell in row:
            r.elements.add(**cell)
    return ds


def _template_definition(*members: pb.Payload.Metric) -> pb.Payload.Metric:
    t = pb.Payload.Template(version="", is_definition=True, metrics=list(members))
    return metric("Motor", 19, timestamp=TS, template_value=t)


CASES: dict[str, bytes] = {
    # property types: each on its own, next to standard ones (the order is a Java HashMap's)
    "property-document": payload(_float_with(("engUnit", _pv(12, string_value="rpm")),
                                             ("doc", _pv(DOCUMENT, string_value='{"a":1,"b":[true,null]}'))), _run()),
    "property-array": payload(_float_with(("arr", _pv(ARRAY, string_value="[1,2,3]")),
                                          ("tooltip", _pv(12, string_value="speed"))), _run()),
    "property-dataset": payload(_float_with(("ds", _pv(DATASET_PROPERTY, string_value='{"columns":[]}')),
                                            ("Quality", _pv(3, int_value=192))), _run()),
    "property-all-three": payload(_float_with(("doc", _pv(DOCUMENT, string_value="{}")),
                                              ("arr", _pv(ARRAY, string_value="[]")),
                                              ("ds", _pv(DATASET_PROPERTY, string_value="")),
                                              ("engUnit", _pv(12, string_value="rpm"))), _run()),
    # the extension types' edge cases
    "property-document-null": payload(_float_with(("doc", _pv(DOCUMENT, is_null=True))), _run()),
    "property-document-no-value": payload(_float_with(("doc", _pv(DOCUMENT))), _run()),
    "property-array-in-int-value": payload(_float_with(("arr", _pv(ARRAY, int_value=7))), _run()),
    "property-unicode": payload(_float_with(("doc", _pv(DOCUMENT, string_value='{"name":"Fräse ✓ 漢字"}'))), _run()),
    # a property that fails: is it dropped alone, or with its metric?
    "property-unknown-type": payload(_float_with(("odd", _pv(99, string_value="x")),
                                                 ("engUnit", _pv(12, string_value="rpm"))), _run()),
    "property-uint32-without-value": payload(_float_with(("bad", _pv(7)),
                                                         ("engUnit", _pv(12, string_value="rpm"))), _run()),
    "property-fewer-values-than-keys": payload(
        metric("Motor/Speed", 9, timestamp=TS, float_value=72.5,
               properties=pb.Payload.PropertySet(keys=["a", "b"], values=[_pv(12, string_value="x")])), _run()),
    # nested property sets
    "property-nested-propertyset": payload(_float_with(
        ("meta", _pv(20, propertyset_value=_props(("doc", _pv(DOCUMENT, string_value="{}")),
                                                  ("engUnit", _pv(12, string_value="rpm")))))), _run()),
    "property-nested-propertysetlist": payload(_float_with(
        ("list", _pv(21, propertysets_value=pb.Payload.PropertySetList(propertyset=[
            _props(("arr", _pv(ARRAY, string_value="[1]"))), _props(("q", _pv(3, int_value=192)))])))), _run()),
    # a template definition whose member carries an extension property (UDTs in an NBIRTH)
    "template-member-property": payload(_template_definition(
        _float_with(("doc", _pv(DOCUMENT, string_value='{"doc":"member"}')), name="Speed"),
        metric("Run", 11, timestamp=TS, boolean_value=False)), _run()),
    # the Color column type
    "dataset-color": payload(metric("Palette", 16, timestamp=TS, dataset_value=_dataset(
        [12, COLOR], [[{"string_value": "red"}, {"int_value": 0xFF0000}],
                      [{"string_value": "half"}, {"int_value": 0x80123456}],
                      [{"string_value": "none"}, {}]])), _run()),
    "dataset-color-only": payload(metric("Colors", 16, timestamp=TS, dataset_value=_dataset(
        [COLOR], [[{"int_value": 0x00FF00}]])), _run()),
    "dataset-color-in-template": payload(_template_definition(
        metric("Palette", 16, timestamp=TS, dataset_value=_dataset([COLOR], [[{"int_value": 0x0000FF}]])),
        metric("Run", 11, timestamp=TS, boolean_value=False)), _run()),
    "dataset-unknown-column-type": payload(metric("Odd", 16, timestamp=TS, dataset_value=_dataset(
        [-10099], [[{"int_value": 1}]])), _run()),
    # alarm content types
    "alarm-string": payload(metric("Alarms/HighTemp", 12, timestamp=TS, string_value='{"state":"ActiveUnacked"}',
                                   metadata=pb.Payload.MetaData(content_type="Alarm")), _run()),
    "alarm-other-datatype": payload(metric("Alarms/HighTemp", 3, timestamp=TS, int_value=5,
                                           string_value='{"state":"Cleared"}',
                                           metadata=pb.Payload.MetaData(content_type="Alarm")), _run()),
    "alarm-no-datatype": payload(metric("Alarms/HighTemp", 0, timestamp=TS, string_value="{}",
                                        metadata=pb.Payload.MetaData(content_type="Alarm")), _run()),
    "alarm-with-properties": payload(metric("Alarms/HighTemp", 12, timestamp=TS, string_value="{}", alias=42,
                                            is_historical=True, metadata=pb.Payload.MetaData(content_type="Alarm"),
                                            properties=_props(("doc", _pv(DOCUMENT, string_value="{}")))), _run()),
    "alarm-reconciliation": payload(metric("Alarms/_reconcile", 16, timestamp=TS,
                                           metadata=pb.Payload.MetaData(content_type="AlarmReconciliation"),
                                           dataset_value=_dataset([12, 13], [[{"string_value": "id-1"},
                                                                              {"long_value": TS}]])), _run()),
    "alarm-reconciliation-other-datatype": payload(metric(
        "Alarms/_reconcile", 12, timestamp=TS, string_value="ignored",
        metadata=pb.Payload.MetaData(content_type="AlarmReconciliation"),
        dataset_value=_dataset([12], [[{"string_value": "id-2"}]])), _run()),
    "alarm-reconciliation-color": payload(metric("Alarms/_reconcile", 16, timestamp=TS,
                                                 metadata=pb.Payload.MetaData(content_type="AlarmReconciliation"),
                                                 dataset_value=_dataset([COLOR], [[{"int_value": 0xFFFFFF}]])), _run()),
    "content-type-other": payload(metric("Doc", 12, timestamp=TS, string_value="{}",
                                         metadata=pb.Payload.MetaData(content_type="application/json")), _run()),
}
