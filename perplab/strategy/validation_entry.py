"""Register and enforce the validator before importing strategy/runtime modules."""
from __future__ import annotations
import sys
from pathlib import Path
from perplab.resources import ResourceGuard, GiB


def main() -> int:
    root = Path(sys.argv[1])
    args = sys.argv[2:]
    request = Path(args[args.index("--request") + 1])
    guard = ResourceGuard(root, "validation", directory=request.parent, nested=True)
    guard.allowance = min(GiB, int(guard.policy.worker_gib * GiB))
    with guard:
        from perplab.strategy.sandbox import main as validate
        return validate(args)


if __name__ == "__main__":
    raise SystemExit(main())
