import os
import pytest


from qcore.utils import load_sim_params as mocked_load_sim_params
from workflow.automation.lib.shared import set_wct as mocked_set_wct

from workflow.automation.tests.test_common_set_up import get_fault_from_rel, set_up

import workflow.automation.submit.submit_hf


@pytest.mark.usefixtures("init_scheduler")
def test_main(set_up, mocker):
    """No return value. Just check that it runs without crashing"""

    mocker.patch(
        "workflow.automation.submit.submit_hf.set_wct", lambda x, y, z: mocked_set_wct(x, y, True)
    )
    mocker.patch("workflow.automation.submit.submit_hf.confirm", lambda x: False)
    mocker.patch(
        "workflow.automation.submit.submit_hf.est.est_HF_chours_single",
        lambda *args, **kwargs: (2, 0.05, 40),
    )

    for root_path, realisation in set_up:

        rel_dir = os.path.join(
            root_path, "CSRoot", "Runs", get_fault_from_rel(realisation), realisation
        )
        # Fault will probably change on each set of data, so reset this every time
        mocker.patch(
            "workflow.automation.submit.submit_hf.utils.load_sim_params",
            lambda x: mocked_load_sim_params(os.path.join(rel_dir, x)),
        )

        workflow.automation.submit.submit_hf.main(
            submit=None,
            machine="default",
            ncores=80,
            rel_dir=rel_dir,
            retries=0,
            seed=None,
            version=None,
            write_directory=None,
        )
