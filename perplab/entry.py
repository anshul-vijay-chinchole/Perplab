"""Lightweight entry: enforce limits before importing data/strategy dependencies."""
from __future__ import annotations
import sys
from pathlib import Path
from perplab.resources import ResourceGuard


def main() -> int:
    argv = sys.argv[1:]
    root = Path("userdata")
    for index, arg in enumerate(argv):
        if arg == "--root" and index + 1 < len(argv):
            root = Path(argv[index + 1])
        elif arg.startswith("--root="):
            root = Path(arg.split("=", 1)[1])
    role = "api" if "serve" in argv else "collector" if "collect" in argv else "cli"
    with ResourceGuard(root, role):
        from perplab.cli import main as run
        return run(argv)
