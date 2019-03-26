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


def test_write_sl_script(set_up):
    """The return value of write_sl_script is a filename that depends on the install location and current time and it
    is there fore not practical to test it with """
    func_name = "submit_hf.py_write_sl_script"
    params = inspect.getfullargspec(scripts.submit_hf.write_sl_script).args
    for root_path, realisation in set_up:

        input_params = get_input_params(root_path, func_name, params)
        for i in range(len(input_params)):
            if isinstance(input_params[i], str) and input_params[i].startswith(
                "CSRoot"
            ):
                input_params[i] = os.path.join(root_path, input_params[i])
        script_location = scripts.submit_hf.write_sl_script(*input_params)
        os.remove(script_location)
