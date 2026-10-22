"""Log files besides the console, or the console only (CONNECTOR_LOG_FILES=false, as in the Docker image)."""
from __future__ import annotations

import os
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
PROBE = """
import logging, sparkplug_connector.__main__ as m
files = [h for h in logging.getLogger().handlers if isinstance(h, logging.FileHandler)]
print(m.LOG_FILES, m.logging_manager is not None, len(files), len(logging.getLogger().handlers))
"""


def probe(**env) -> list[str]:
    out = subprocess.run([sys.executable, "-c", PROBE], cwd=ROOT, capture_output=True, text=True, check=True,
                         env={**os.environ, **env})
    return out.stdout.split()


def test_log_files_by_default():
    assert probe(CONNECTOR_LOG_FILES="true") == ["True", "True", "1", "2"]       # console + rotating file


def test_console_only_when_switched_off():
    assert probe(CONNECTOR_LOG_FILES="false") == ["False", "False", "0", "1"]
