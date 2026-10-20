"""tests/sparkplug: the repository root on sys.path, and the Java Tahu reference as a fixture."""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"
EXPECTED = pathlib.Path(__file__).resolve().parent / "expected"


@pytest.fixture(scope="session")
def java_tahu():
    """Java Tahu on Java 17 (skips the test when it can't run here)."""
    import java_tahu as reference

    reason = reference.unavailable_reason()
    if reason:
        pytest.skip(reason)
    if not reference.java_version().startswith("17."):
        pytest.skip(f"the reference must run on Java 17 (the output the connector reproduces), not {reference.java_version()} (set TAHU_JVM)")
    return reference
