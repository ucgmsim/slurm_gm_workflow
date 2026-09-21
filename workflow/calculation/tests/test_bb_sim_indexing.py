"""Guards against the indexing bug class that broke the previous patch.

bb_sim.py cannot be imported here - it pulls in mpi4py and qcore - so
these read the source. Crude, but they catch exactly the mistake that
made the captured in-place patch wrong: sizing the output from
lf.stations while writing rows at hf.stations indices.
"""

from pathlib import Path

import pytest

BB_SIM = Path(__file__).resolve().parents[1] / "bb_sim.py"


@pytest.fixture(scope="module")
def source() -> str:
    return BB_SIM.read_text()


@pytest.mark.parametrize(
    "forbidden",
    ["lf.stations.size", "hf.stations.size", "lf.nstat", "hf.nstat"],
)
def test_output_layout_never_keys_off_a_raw_station_array(source, forbidden):
    assert forbidden not in source, (
        f"{forbidden} still drives part of bb_sim.py. Every quantity must be "
        "keyed by the canonical station set (n_bb / lf_idx / hf_idx) instead."
    )


def test_resolver_is_used(source):
    assert "resolve_station_set" in source
    assert "from workflow.calculation.bb_station_set import" in source


def test_new_flags_are_exposed(source):
    assert '"--station-list"' in source
    assert '"--allow-station-subset"' in source


def test_checkpointing_is_still_present(source):
    # The previous patch removed this. Runs of this size need resume.
    assert "def unfinished(" in source
    assert "Checkpoints found." in source


def test_old_aborting_checks_are_gone(source):
    assert "LF nstat != HF nstat" not in source
    assert "LF and HF were run with different station files" not in source
