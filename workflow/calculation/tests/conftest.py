"""Make `workflow.*` resolve to this repository during tests.

The repository root carries an __init__.py, so pytest treats the repo
itself as a package and walks up to its parent directory to set the
import root. That parent also holds an unrelated project named
`workflow`, which then shadows this one and makes
`workflow.calculation` unimportable.

Putting the repository root first on sys.path resolves it to this repo,
which is also how bb_sim.py is imported at runtime (PYTHONPATH set to
the checkout root).
"""

import sys
from pathlib import Path

REPO_ROOT = str(Path(__file__).resolve().parents[3])

if sys.path and sys.path[0] != REPO_ROOT:
    sys.path.insert(0, REPO_ROOT)
