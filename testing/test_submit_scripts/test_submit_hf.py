import inspect
import os

from qcore.utils import load_sim_params as mocked_load_sim_params
from shared_workflow.shared import set_wct as mocked_set_wct

from testing.test_common_set_up import set_up, get_input_params, get_fault_from_rel

import scripts.submit_hf


def test_main(set_up, mocker):
    """No return value. Just check that it runs without crashing"""
    function = "main"
    params = inspect.getfullargspec(scripts.submit_hf.main).args

    mocker.patch(
        "scripts.submit_hf.set_wct", lambda x, y, z: mocked_set_wct(x, y, True)
    )
    mocker.patch("scripts.submit_hf.confirm", lambda x: False)
    mocker.patch(
        "scripts.submit_hf.est.est_HF_chours_single",
        lambda *args, **kwargs: (2, 0.05, 40),
    )
    mocker.patch(
        "shared_workflow.shared_template.recipe_dir",
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "..", "..", "templates"
        ),
    )

    for root_path, realisation in set_up:
        args = get_input_params(
            root_path, "{}_{}".format("submit_hf.py", function), params
        )

        # Fault will probably change on each set of data, so reset this every time
        mocker.patch(
            "scripts.submit_hf.utils.load_sim_params",
            lambda x: mocked_load_sim_params(
                os.path.join(
                    root_path,
                    "CSRoot",
                    "Runs",
                    get_fault_from_rel(realisation),
                    realisation,
                    x,
                )
            ),
        )

        scripts.submit_hf.main(*args)
