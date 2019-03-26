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


def test_write_sl_script(set_up, mocker):
    """The return value of write_sl_script is a filename that depends on the install location and current time and it
    is there fore not practical to test it with """
    func_name = "submit_emod3d.py_write_sl_script"
    params = inspect.getfullargspec(scripts.submit_emod3d.write_sl_script).args

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
        fault = get_fault_from_rel(realisation)

        mocker.patch(
            "scripts.submit_emod3d.utils.load_sim_params",
            lambda x: mocked_load_sim_params(
                os.path.join(root_path, "CSRoot", "Runs", fault, realisation, x)
            ),
        )

        input_params = get_input_params(root_path, func_name, params)
        for i in range(len(input_params)):
            if isinstance(input_params[i], str) and input_params[i].startswith(
                "CSRoot"
            ):
                input_params[i] = os.path.join(root_path, input_params[i])
        script_location = scripts.submit_emod3d.write_sl_script(*input_params)
        os.remove(script_location)
