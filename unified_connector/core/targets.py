"""A source's Zerobus target table, and the rule that Sparkplug rows and other rows don't share one.

A source's table: its own ``target_table`` (``catalog.schema.table``), else ``zerobus.source_targets.<name>``
(merged over the default), else ``zerobus.default_target``. Sparkplug rows have the ``sparkplug_raw`` columns
and the other protocols' rows a different schema, so a table can take one kind or the other: Zerobus refuses
a row with columns the table doesn't have, and one refused row fails its whole batch, which a confirmed
writer resends for good (the table's other rows wait behind it).
"""
from __future__ import annotations

import re
from typing import Any, Callable, Dict, Iterable, List, Tuple

SPARKPLUG_PROTOCOLS = ("sparkplug", "sparkplug_b")
_PART = re.compile(r"[^.\s`]+")


def parse_target_table(text: Any) -> Tuple[str, str, str]:
    """``catalog.schema.table`` as its three parts; raises ``ValueError`` otherwise."""
    parts = str(text).strip().split(".")
    if len(parts) != 3 or not all(_PART.fullmatch(p) for p in parts):
        raise ValueError(f"target table must be catalog.schema.table: {text}")
    return parts[0], parts[1], parts[2]


def target_config(source: Dict[str, Any], zerobus_config: Dict[str, Any]) -> Dict[str, Any]:
    """The target (workspace, catalog, schema, table) a source writes to."""
    default = zerobus_config.get("default_target", {}) or {}
    own = str(source.get("target_table") or "").strip()
    if own:
        catalog, schema, table = parse_target_table(own)
        return {**default, "catalog": catalog, "schema": schema, "table": table}
    override = (zerobus_config.get("source_targets") or {}).get(source.get("name"))
    return {**default, **override} if override else default


def is_sparkplug(source: Dict[str, Any]) -> bool:
    return str(source.get("protocol", "")).lower() in SPARKPLUG_PROTOCOLS


def table_name(target: Dict[str, Any]) -> str:
    return f"{target.get('catalog', 'main')}.{target.get('schema', 'iot_data')}.{target.get('table', 'sensor_readings')}"


def mixed_tables(sources: Iterable[Dict[str, Any]], table_of: Callable[[Dict[str, Any]], Tuple[str, str]]) -> List[str]:
    """For each table that would get Sparkplug rows and other rows, a sentence naming the sources.
    ``table_of(source)`` is (the table's key, e.g. the bridge's target id; its name for the message)."""
    kinds: Dict[str, Tuple[str, List[str], List[str]]] = {}
    for source in sources:
        key, label = table_of(source)
        _, sparkplug, other = kinds.setdefault(key, (label, [], []))
        (sparkplug if is_sparkplug(source) else other).append(str(source.get("name")))
    return [f"table {label} would get Sparkplug rows ({', '.join(sparkplug)}) and other rows ({', '.join(other)}): "
            "give one of them a target table of its own"
            for _, (label, sparkplug, other) in sorted(kinds.items()) if sparkplug and other]
