"""The TCK's fixtures: HiveMQ CE with the Eclipse Sparkplug TCK extension, and a Mosquitto as the second MQTT server.
Imported by the test module (a second ``conftest.py`` would shadow ``tests/conftest.py``, which tests import by name).

Needs Docker and the images ``hivemq/hivemq-ce:2022.1`` (the version the TCK's user guide names; amd64 only, emulated
on arm64) and ``eclipse-mosquitto:2`` (skipped otherwise). The TCK (EPL-2.0) isn't in this repository: its packaged
release is downloaded once into ``~/.cache/sparkplug-tck`` (``SPARKPLUG_TCK_CACHE``) and checked against its SHA-256.
Results go to ``tests/tck/results/`` (``TCK_RESULTS_DIR``): one JSON per test, ``summary.md``, and the TCK's own
``SparkplugTCKresults.log``.
"""
from __future__ import annotations

import hashlib
import os
import pathlib
import shutil
import socket
import subprocess
import time
import urllib.request
import zipfile

import pytest

import tck_driver

TCK_URL = "https://download.eclipse.org/sparkplug/3.0.0/Eclipse-Sparkplug-TCK-3.0.0.zip"
TCK_SHA256 = "a70b2c2f00d67ac714eadd5ac50f6241e0efa26036d95ca8ec667d491021b86b"
HIVEMQ_IMAGE = "hivemq/hivemq-ce:2022.1"
MOSQUITTO_IMAGE = "eclipse-mosquitto:2"
CACHE = pathlib.Path(os.environ.get("SPARKPLUG_TCK_CACHE", "~/.cache/sparkplug-tck")).expanduser()
RESULTS = pathlib.Path(os.environ.get("TCK_RESULTS_DIR", pathlib.Path(__file__).parent / "results"))
STARTUP_S = 180.0


def _sha256(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _require_docker() -> None:
    """Skips unless Docker runs and has both images (before anything is downloaded)."""
    if shutil.which("docker") is None or subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        pytest.skip("Docker not available")
    for image in (HIVEMQ_IMAGE, MOSQUITTO_IMAGE):
        if subprocess.run(["docker", "image", "inspect", image], capture_output=True).returncode != 0:
            pytest.skip(f"{image} image not available locally (docker pull {image})")


@pytest.fixture(scope="session")
def tck_package(tmp_path_factory) -> pathlib.Path:
    """The TCK's HiveMQ extension folder and HiveMQ config (``sparkplug-tck/``, ``config.xml``), from the release."""
    _require_docker()
    archive = CACHE / TCK_URL.rsplit("/", 1)[1]
    if not archive.exists():
        CACHE.mkdir(parents=True, exist_ok=True)
        try:
            urllib.request.urlretrieve(TCK_URL, archive.with_suffix(".part"))
        except OSError as e:
            pytest.skip(f"the Sparkplug TCK couldn't be downloaded from {TCK_URL}: {e}")
        archive.with_suffix(".part").rename(archive)
    if _sha256(archive) != TCK_SHA256:
        archive.unlink()                # the next run downloads it again
        pytest.fail(f"{archive}: SHA-256 is not the 3.0.0 release's {TCK_SHA256} (deleted)")
    out = tmp_path_factory.mktemp("tck")
    with zipfile.ZipFile(archive) as z:
        z.extract("SparkplugTCK/hivemq-extension/sparkplug-tck-3.0.0.zip", out)
        z.extract("SparkplugTCK/hivemq-configuration/config.xml", out)
    with zipfile.ZipFile(out / "SparkplugTCK/hivemq-extension/sparkplug-tck-3.0.0.zip") as z:
        z.extractall(out)
    shutil.copy(out / "SparkplugTCK/hivemq-configuration/config.xml", out / "config.xml")
    return out


def _docker(*args: str, check: bool = True) -> str:
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=check).stdout.strip()


def _port(container: str) -> int:
    return int(_docker("port", container, "1883/tcp").splitlines()[0].rsplit(":", 1)[1])


def _wait_tcp(port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


@pytest.fixture(scope="session")
def brokers(tck_package):
    """HiveMQ with the TCK (port on 127.0.0.1) and Mosquitto (port, and ``tcp://mosquitto:1883`` for the TCK)."""
    tag = f"spz-tck-{os.getpid()}"
    _docker("network", "create", tag)
    hivemq = mosquitto = None
    try:
        mosquitto = _docker("run", "-d", "--rm", "--network", tag, "--network-alias", "mosquitto", "-p", "127.0.0.1::1883",
                            MOSQUITTO_IMAGE, "mosquitto", "-c", "/mosquitto-no-auth.conf")
        hivemq = _docker("run", "-d", "--platform", "linux/amd64", "--network", tag, "-p", "127.0.0.1::1883",
                         "-v", f"{tck_package / 'sparkplug-tck'}:/opt/hivemq/extensions/sparkplug-tck:ro",
                         "-v", f"{tck_package / 'config.xml'}:/opt/hivemq/conf/config.xml:ro", HIVEMQ_IMAGE)
        deadline = time.monotonic() + STARTUP_S
        while "Started HiveMQ" not in _docker("logs", hivemq, check=False):
            if time.monotonic() > deadline:
                pytest.fail(f"HiveMQ didn't start in {STARTUP_S:.0f} s")
            time.sleep(1)
        if "Sparkplug TCK Extension" not in _docker("logs", hivemq, check=False):
            pytest.fail("HiveMQ started without the Sparkplug TCK extension")
        ports = {"hivemq": _port(hivemq), "mosquitto": _port(mosquitto), "mosquitto_url": "tcp://mosquitto:1883"}
        for p in (ports["hivemq"], ports["mosquitto"]):
            _wait_tcp(p, 30)
        yield ports
    finally:
        if hivemq:
            RESULTS.mkdir(parents=True, exist_ok=True)
            subprocess.run(["docker", "cp", f"{hivemq}:/opt/hivemq/SparkplugTCKresults.log",
                            str(RESULTS / "SparkplugTCKresults.log")], capture_output=True)
            (RESULTS / "hivemq.log").write_text(_docker("logs", hivemq, check=False))
            _docker("rm", "-f", hivemq, check=False)
        if mosquitto:
            _docker("rm", "-f", mosquitto, check=False)
        _docker("network", "rm", tag, check=False)


@pytest.fixture(scope="session")
def tck_results():
    """Each test's results, written to ``RESULTS`` as the run goes (JSON per test, ``summary.md``)."""
    results: dict[str, dict] = {}

    def record(test: str, result: tck_driver.Result) -> None:
        RESULTS.mkdir(parents=True, exist_ok=True)
        results[test] = result.to_json()
        (RESULTS / f"{test}.json").write_text(tck_driver.dumps({**result.to_json(), "details": result.details}))
        (RESULTS / "summary.md").write_text(tck_driver.summary_markdown(results))
    return record


@pytest.fixture
async def tck(brokers):
    async with tck_driver.Tck(brokers["hivemq"]) as driver:
        yield driver
