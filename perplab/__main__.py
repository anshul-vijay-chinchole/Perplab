"""Enable `python -m perplab`.

The watchdog scheduled task invokes the collector this way, so this module is load-bearing
for unattended operation rather than a convenience.
"""

from perplab.cli import main

raise SystemExit(main())
