"""The queue between a source and its table's writer: bounded, in memory, with a drop policy."""
from __future__ import annotations

import logging

import pytest

from sparkplug_connector.core.backpressure import BackpressureManager


def queue(size: int, policy: str) -> tuple[BackpressureManager, list]:
    q = BackpressureManager({"memory_queue": {"max_size": size, "drop_policy": policy}})
    dropped: list = []
    q.on_drop = dropped.append
    return q, dropped


async def drain(q: BackpressureManager) -> list:
    out = []
    while (r := await q.dequeue()) is not None:
        out.append(r)
    return out


async def test_oldest_drops_the_head_and_keeps_the_new_record():
    q, dropped = queue(2, "oldest")
    for n in range(4):
        assert await q.enqueue({"n": n})
    assert dropped == [{"n": 0}, {"n": 1}]
    assert await drain(q) == [{"n": 2}, {"n": 3}]
    assert q.get_metrics() == {"records_enqueued": 4, "records_dequeued": 2, "records_dropped": 2, "current_queue_depth": 0}


async def test_newest_refuses_the_new_record():
    q, dropped = queue(2, "newest")
    assert [await q.enqueue({"n": n}) for n in range(3)] == [True, True, False]
    assert dropped == [{"n": 2}]
    assert await drain(q) == [{"n": 0}, {"n": 1}]


async def test_reject_raises_and_leaves_the_record_to_the_caller():
    q, dropped = queue(1, "reject")
    assert await q.enqueue({"n": 0})
    with pytest.raises(ValueError, match="full"):
        await q.enqueue({"n": 1})
    assert dropped == [] and await drain(q) == [{"n": 0}]


async def test_on_drop_failing_doesnt_stop_the_queue(caplog):
    q, _ = queue(1, "oldest")

    def broken(record):
        raise RuntimeError("log closed")
    q.on_drop = broken
    await q.enqueue({"n": 0})
    assert await q.enqueue({"n": 1})
    assert await drain(q) == [{"n": 1}] and "on_drop failed" in caplog.text


def test_a_disk_spool_in_the_config_is_reported_and_ignored(caplog):
    with caplog.at_level(logging.WARNING):
        BackpressureManager({"disk_spool": {"enabled": False}})
    assert "disk_spool" not in caplog.text
    with caplog.at_level(logging.WARNING):
        q = BackpressureManager({"disk_spool": {"enabled": True, "path": "spool"}})
    assert "disk_spool is not supported" in caplog.text
    assert q.max_queue_size == 10000 and q.drop_policy.value == "oldest"
