import inspect
import os

from qcore.utils import load_sim_params as mocked_load_sim_params
from qcore.utils import load_yaml as mocked_load_yaml
from shared_workflow.shared import set_wct as mocked_set_wct

from testing.test_common_set_up import set_up, get_input_params, get_fault_from_rel

import scripts.submit_emod3d


def test_main(set_up, mocker):
    """No return value. Just check that it runs without crashing"""
    function = "main"
    params = inspect.getfullargspec(scripts.submit_emod3d.main).args

    mocker.patch(
        "scripts.submit_emod3d.set_wct", lambda x, y, z: mocked_set_wct(x, y, True)
    )
    mocker.patch("scripts.submit_emod3d.confirm", lambda x: False)
    mocker.patch(
        "scripts.submit_emod3d.est.est_LF_chours_single",
        lambda a, b, c, d, e, f, g: (2, 0.05, 40),
    )
    mocker.patch(
        "scripts.set_runparams.utils.load_yaml",
        lambda x: mocked_load_yaml(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "..",
                "..",
                "templates",
                "gmsim",
                "16.1",
                "emod3d_defaults.yaml",
            )
        )
        if "emod3d_defaults.yaml" in x
        else mocked_load_yaml(x),
    )

    for root_path, realisation in set_up:
        args = get_input_params(
            root_path, "{}_{}".format("submit_emod3d.py", function), params
        )

        # Fault will probably change on each set of data, so reset these every time
        mocker.patch(
            "scripts.submit_emod3d.utils.load_sim_params",
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

        scripts.submit_emod3d.main(*args)
