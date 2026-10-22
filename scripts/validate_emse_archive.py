"""Compatibility import for the recorded archive-check regression test."""
from pathlib import Path
import runpy

_module = runpy.run_path(str(Path(__file__).with_name("validate_artifact.py")))
checked_summary = _module["checked_summary"]

if __name__ == "__main__":
    _module["main"]()
