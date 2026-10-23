"""The Eclipse Sparkplug TCK's six Host Application tests against the connector as primary host.

Each test runs the TCK test the way its user guide says (the host started before or after the test, stopped, asked
something) and compares every assertion's result with ``expected/<test>.json``: what the connector passes, and what
it doesn't, is recorded there and in the README. ``TCK_RECORD=1`` writes the expectations from the run instead.
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib

import pytest

from tck_driver import connector, dumps, source
from tck_fixtures import brokers, tck, tck_package, tck_results  # noqa: F401 - the fixtures

pytestmark = pytest.mark.tck

HOST_ID = "tck-host"
GROUP, EDGE, DEVICE = "TckGroup", "TckEdge", "TckDevice"
REORDER_TIMEOUT_MS = 5000          # the TCK console's default, and the connector's
EXPECTED = pathlib.Path(__file__).parent / "expected"


def check(test: str, result, tck_results) -> None:
    tck_results(test, result)
    path = EXPECTED / f"{test}.json"
    if os.environ.get("TCK_RECORD") == "1":
        EXPECTED.mkdir(exist_ok=True)
        path.write_text(dumps(result.assertions))
        return
    expected = json.loads(path.read_text())
    differences = {k: (expected.get(k), result.assertions.get(k)) for k in expected.keys() | result.assertions.keys()
                   if expected.get(k) != result.assertions.get(k)}
    assert not differences, (f"{test}: (expected, got) {dumps(differences)}"
                             f"failures: {dumps({k: result.details[k] for k in differences if k in result.details})}")


async def test_session_establishment(tck, brokers, tck_results):
    """The host started after the test: will, subscriptions, ONLINE, and ONLINE again after the TCK's OFFLINE."""
    await tck.start("SessionEstablishmentTest", HOST_ID)
    async with connector(source("hivemq", brokers["hivemq"], HOST_ID)):
        result = await tck.result(30)
    check("SessionEstablishmentTest", result, tck_results)


async def test_session_termination(tck, brokers, tck_results):
    """The host started after the test, then stopped: OFFLINE before a clean DISCONNECT."""
    await tck.start("SessionTerminationTest", HOST_ID)
    async with connector(source("hivemq", brokers["hivemq"], HOST_ID)):
        await asyncio.sleep(1)
    check("SessionTerminationTest", await tck.result(30), tck_results)


async def test_message_ordering(tck, brokers, tck_results):
    """The TCK's edge node skips a ``seq``: a rebirth request within twice the reorder timeout, none for a message
    only a little late."""
    async with connector(source("hivemq", brokers["hivemq"], HOST_ID, sequence_reordering_timeout=REORDER_TIMEOUT_MS)):
        await tck.start("MessageOrderingTest", HOST_ID, GROUP, EDGE, DEVICE, str(REORDER_TIMEOUT_MS))
        result = await tck.result(90)
    check("MessageOrderingTest", result, tck_results)


async def test_multiple_brokers(tck, brokers, tck_results):
    """One primary host id on two MQTT servers (a source each): ONLINE on both, and back after the TCK drops one."""
    await tck.start("MultipleBrokerTest", HOST_ID, brokers["mosquitto_url"])
    async with connector(source("hivemq", brokers["hivemq"], HOST_ID), source("mosquitto", brokers["mosquitto"], HOST_ID)):
        result = await tck.result(30)
    check("MultipleBrokerTest", result, tck_results)


async def test_send_command(tck, brokers, tck_results):
    """The operator is asked to make the host send an NCMD rebirth, an NCMD write, a DCMD rebirth and a DCMD write,
    in that order (each step waits for the previous one's command)."""
    async with connector(source("hivemq", brokers["hivemq"], HOST_ID)):
        await tck.start("SendCommandTest", HOST_ID, GROUP, EDGE, DEVICE)
        assert await tck.prompt(30) == f"Send an edge rebirth to edge node {EDGE}"
        await asyncio.sleep(3)              # the connector has no way to send one on request
        await tck.end()
        result = await tck.result(30)
    check("SendCommandTest", result, tck_results)


async def test_edge_session_termination(tck, brokers, tck_results):
    """The TCK's device dies, then its edge node: six questions, does the host mark them offline (and their
    metrics STALE)."""
    async with connector(source("hivemq", brokers["hivemq"], HOST_ID)):
        await tck.start("EdgeSessionTerminationTest", HOST_ID, GROUP, EDGE, DEVICE)
        for _ in range(6):
            await tck.prompt(30)
            await tck.reply(False)          # the connector shows no edge node or device state
        result = await tck.result(30)
    check("EdgeSessionTerminationTest", result, tck_results)
