import inspect
import os
import glob
import pickle

from qcore.config import host
from qcore.utils import load_sim_params as mocked_load_sim_params
from shared_workflow.shared import set_wct as mocked_set_wct
from shared_workflow.shared import resolve_header as mocked_resolve_header
from estimation.estimate_wct import est_HF_chours_single as mocked_est_HF_chours_single
from qcore.utils import load_yaml as mocked_load_yaml

from testing.test_common_set_up import (
    INPUT,
    OUTPUT,
    set_up,
    get_bench_output,
    get_input_params,
    get_fault_from_rel,
)

import scripts.submit_hf


def test_main(set_up, mocker):
    """No return value. Just check that it runs without crashing"""
    function = "main"

    mocker.patch(
        "scripts.submit_hf.set_wct", lambda x, y, z: mocked_set_wct(x, y, True)
    )
    mocker.patch("scripts.submit_hf.confirm", lambda x: False)

    for root_path, realisation in set_up:
        fault = get_fault_from_rel(realisation)
        file_path = os.path.join(
            root_path, INPUT, "{}_{}_{}.P".format("submit_hf.py", function, "args")
        )
        with open(file_path, "rb") as load_file:
            args = pickle.load(load_file)

        # Fault will probably change on each set of data, so reset these every time
        mocker.patch(
            "scripts.submit_hf.utils.load_sim_params",
            lambda x: mocked_load_sim_params(
                os.path.join(root_path, "CSRoot", "Runs", fault, realisation, x)
            ),
        )

        mocker.patch(
            "scripts.submit_hf.est.est_HF_chours_single",
            lambda *args, **kwargs: mocked_est_HF_chours_single(
                *args,
                *kwargs,
                model_dir=os.path.join(root_path, "AdditionalData", "models", "HF")
            ),
        )

        scripts.submit_hf.main(args)


def test_write_sl_script(set_up, mocker):
    """The return value of write_sl_script is a filename that depends on the install location and current time and it
    is there fore not practical to test it with """
    func_name = "submit_hf.py_write_sl_script"
    params = inspect.getfullargspec(scripts.submit_hf.write_sl_script).args
    for root_path, realisation in set_up:
        fault = get_fault_from_rel(realisation)

        input_params = get_input_params(root_path, func_name, params)
        for i in range(len(input_params)):
            if isinstance(input_params[i], str) and input_params[i].startswith(
                "CSRoot"
            ):
                input_params[i] = os.path.join(root_path, input_params[i])
        script_location = scripts.submit_hf.write_sl_script(*input_params)
        os.remove(script_location)
