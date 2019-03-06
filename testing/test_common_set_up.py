import pytest

from qcore import utils

INPUT = "input"
OUTPUT = "output"
REALISATIONS = [
    (
        "PangopangoF29_HYP01-10_S1244",
        "http://ec2-54-206-55-199.ap-southeast-2.compute.amazonaws.com/static/public/testing/slurm_gm_workflow/PangopangoF29_HYP01-10_S1244.zip",
    )
]


@pytest.yield_fixture(scope="session", autouse=True)
def set_up(request):
    return utils.test_set_up(REALISATIONS)
