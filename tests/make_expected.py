"""Write Java Tahu's JSON (run on Java 17) for the fixtures of make_fixtures.py: expected/<fixture>.json for
each fixtures/transmission/*.bin, and the golden sample fixtures/sample_dbirth.json (keys sorted and
indented, as a table that sorts object keys stores it).

    TAHU_JVM=<Java 17 libjvm> .venv/bin/python tests/make_expected.py
"""
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
import java_tahu  # noqa: E402
import make_fixtures  # noqa: E402

if not java_tahu.java_version().startswith("17."):
    sys.exit(f"Java 17 expected (the output the connector reproduces), got {java_tahu.java_version()}; set TAHU_JVM")
(HERE / "expected").mkdir(exist_ok=True)
for f in sorted(make_fixtures.OUT.glob("*.bin")):
    (HERE / "expected" / f"{f.stem}.json").write_text(java_tahu.to_tahu_json(f.read_bytes()), encoding="utf-8")
    print("wrote", f.stem)
golden = json.loads(java_tahu.to_tahu_json(make_fixtures.golden_dbirth()))
(HERE / "fixtures" / "sample_dbirth.json").write_text(json.dumps(golden, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
print("wrote sample_dbirth")
print(f"Java {java_tahu.java_version()} at {java_tahu.java_home()}")
