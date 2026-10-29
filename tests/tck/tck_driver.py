"""The Eclipse Sparkplug TCK driven over MQTT, and the connector as the host application under test.

The TCK is a HiveMQ broker extension: it watches every MQTT packet of the host under test, plays simulated edge nodes
and devices itself, and checks the specification's ``tck-id-...`` assertions. It is driven over MQTT topics (its web
console is only a front end): a test starts with ``NEW_TEST host <test> <parameters>`` on
``SPARKPLUG_TCK/TEST_CONTROL`` (``END_TEST`` ends it), questions for the operator come on
``SPARKPLUG_TCK/CONSOLE_PROMPT`` (answered ``PASS`` or ``FAIL`` on ``SPARKPLUG_TCK/CONSOLE_REPLY``), and each test's
results on ``SPARKPLUG_TCK/RESULT``: a summary line, then ``<assertion id>: <result>;`` lines and ``OVERALL: <result>;``.

The connector runs in-process (the bridge with its Sparkplug sources, Zerobus faked: rows go nowhere, so ONLINE as
soon as the bridge starts), as in ``tests/test_primary_host.py``.
"""
from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import time
from typing import Any, AsyncIterator
from unittest import mock

import aiomqtt

from sparkplug_connector.core import bridge as core_bridge

TEST_CONTROL = "SPARKPLUG_TCK/TEST_CONTROL"
RESULT = "SPARKPLUG_TCK/RESULT"
LOG = "SPARKPLUG_TCK/LOG"
PROMPT = "SPARKPLUG_TCK/CONSOLE_PROMPT"
REPLY = "SPARKPLUG_TCK/CONSOLE_REPLY"

# a result's first words (a FAIL goes on with the requirement and where it was checked)
STATUSES = ("NOT YET IMPLEMENTED", "NOT EXECUTED", "PASS", "FAIL", "MAYBE", "EMPTY")


class TestNotStarted(Exception):
    """The TCK refused to start the test (e.g. the host wasn't ONLINE, or was when it mustn't be)."""


@dataclasses.dataclass
class Result:
    """One test's results: each assertion's status (``PASS``, ``FAIL``, ``NOT EXECUTED``...) and the TCK's text."""
    name: str
    overall: str
    details: dict[str, str]
    raw: str

    @property
    def assertions(self) -> dict[str, str]:
        """The statuses, without the monitor's assertions the test didn't exercise."""
        return {k: status(v) for k, v in self.details.items()
                if not (k.startswith("Monitor:") and status(v) == "NOT EXECUTED")}

    def to_json(self) -> dict[str, Any]:
        return {"name": self.name, "overall": self.overall, "assertions": self.assertions,
                "failures": {k: v for k, v in self.details.items() if status(v) == "FAIL"}}


def status(value: str) -> str:
    return next((s for s in STATUSES if value.startswith(s)), value)


def parse_result(text: str) -> Result:
    lines = [line.strip() for line in text.strip().splitlines() if line.strip()]
    name = lines[0].split(" Summary Test Results for ", 1)[-1].strip()
    details: dict[str, str] = {}
    overall = ""
    for line in lines[1:]:
        key, _, value = line.partition(": ")
        value = value[:-1] if value.endswith(";") else value
        if key == "OVERALL":
            overall = value
        else:
            details[key] = value
    return Result(name, overall, details, text)


class Tck:
    """The TCK's control topics on the HiveMQ that runs it."""

    def __init__(self, port: int):
        self.port = port
        self.log: list[str] = []
        self._results: asyncio.Queue[Result | TestNotStarted] = asyncio.Queue()
        self._prompts: asyncio.Queue[str] = asyncio.Queue()
        self._client: aiomqtt.Client | None = None
        self._listener: asyncio.Task | None = None

    async def __aenter__(self) -> "Tck":
        self._client = aiomqtt.Client("127.0.0.1", self.port, identifier=f"tck-driver-{time.monotonic_ns()}")
        await self._client.__aenter__()
        for topic in (RESULT, LOG, PROMPT):
            await self._client.subscribe(topic, qos=1)
        self._listener = asyncio.create_task(self._listen())
        return self

    async def __aexit__(self, *exc) -> None:
        if self._listener is not None:
            self._listener.cancel()
            await asyncio.gather(self._listener, return_exceptions=True)
        if self._client is not None:
            await self._client.__aexit__(None, None, None)

    async def _listen(self) -> None:
        try:
            await self._receive()
        except aiomqtt.MqttError:       # the broker went away (the end of the session): nothing more to collect
            pass

    async def _receive(self) -> None:
        async for m in self._client.messages:
            text = bytes(m.payload).decode("utf-8", "replace")
            if m.topic.value == RESULT:
                self._results.put_nowait(parse_result(text))
            elif m.topic.value == PROMPT:
                self._prompts.put_nowait(text)
            else:
                self.log.append(text)
                if "OVERALL: NOT EXECUTED" in text:      # the TCK's answer when a test's constructor refused
                    self._results.put_nowait(TestNotStarted("\n".join(self.log[-5:])))

    async def start(self, test: str, *params: str) -> None:
        for q in (self._results, self._prompts):
            while not q.empty():
                q.get_nowait()
        self.log.clear()
        await self._client.publish(TEST_CONTROL, " ".join(("NEW_TEST", "host", test, *params)), qos=1)

    async def end(self) -> None:
        await self._client.publish(TEST_CONTROL, "END_TEST", qos=1)

    async def prompt(self, timeout: float = 30.0) -> str:
        return await asyncio.wait_for(self._prompts.get(), timeout)

    async def reply(self, passed: bool) -> None:
        await self._client.publish(REPLY, "PASS" if passed else "FAIL", qos=1)

    async def result(self, timeout: float = 30.0) -> Result:
        r = await asyncio.wait_for(self._results.get(), timeout)
        if isinstance(r, TestNotStarted):
            raise r
        return r


def source(name: str, port: int, host_id: str, **settings: Any) -> dict[str, Any]:
    """A Sparkplug source of the connector, primary host ``host_id``, on the MQTT server at ``127.0.0.1:port``."""
    return {"name": name, "protocol": "sparkplug", "endpoint": f"mqtt://127.0.0.1:{port}",
            "client_id": f"tck-{name}-{time.monotonic_ns()}", "primary_host_id": host_id, **settings}


@contextlib.asynccontextmanager
async def connector(*sources: dict[str, Any], online_timeout: float = 20.0) -> AsyncIterator[core_bridge.Bridge]:
    """The connector's bridge with these sources, every one ONLINE; stopped (OFFLINE published) on the way out."""
    class FakeStream:
        async def ingest(self, records):
            return 0

        async def wait_for(self, offset):
            pass

        async def close(self):
            pass

    class FakeRowStreams:
        def __init__(self, *args):
            pass

        async def open(self):
            return FakeStream()

    with mock.patch.object(core_bridge, "ZerobusRowStreams", FakeRowStreams):
        bridge = core_bridge.Bridge({
            "sources": list(sources),
            "zerobus": {"enabled": True, "zerobus_endpoint": "https://example.zerobus",
                        "auth": {"client_id": "id", "client_secret": "secret"},
                        "default_target": {"workspace_host": "https://example.cloud.databricks.com", "catalog": "c",
                                           "schema": "s", "table": "raw_messages"}},
            "backpressure": {},
        })
        await bridge.start()
        try:
            deadline = time.monotonic() + online_timeout
            while not all(s["name"] in bridge.clients and bridge.clients[s["name"]].is_online for s in sources):
                if time.monotonic() > deadline:
                    raise TimeoutError(f"not ONLINE: {[s['name'] for s in sources]}")
                await asyncio.sleep(0.05)
            yield bridge
        finally:
            await bridge.stop()


def summary_markdown(results: dict[str, dict[str, Any]]) -> str:
    """The run as a table: each test's overall result and assertion counts, then the failures with the TCK's text."""
    lines = ["| Test | Overall | Pass | Fail | Not executed |", "|---|---|---|---|---|"]
    for test, r in results.items():
        counts = {s: sum(1 for v in r["assertions"].values() if v == s) for s in ("PASS", "FAIL", "NOT EXECUTED")}
        lines.append(f"| {test} | {r['overall']} | {counts['PASS']} | {counts['FAIL']} | {counts['NOT EXECUTED']} |")
    for test, r in results.items():
        if r["failures"]:
            lines += ["", f"## {test}: failures", ""]
            lines += [f"- `{k}`: {v}" for k, v in sorted(r["failures"].items())]
    return "\n".join(lines) + "\n"


def dumps(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"
