import inspect
import os
import pickle
import filecmp

from qcore.utils import load_sim_params as mocked_load_sim_params
from qcore.utils import load_yaml as mocked_load_yaml
from shared_workflow.shared import write_to_py as mocked_write_to_py

from scripts import set_runparams
from testing.test_common_set_up import (
    INPUT,
    OUTPUT,
    set_up,
    get_input_params,
    get_bench_output,
    get_fault_from_rel,
)


def test_create_run_params(set_up, mocker):
    func_name = "create_run_params"
    generated_file_name = "e3d.par"
    params = inspect.getfullargspec(set_runparams.create_run_params).args
    for root_path, realisation in set_up:
        fault = get_fault_from_rel(realisation)
        get_mocked_sim_params = lambda x: mocked_load_sim_params(
            os.path.join(root_path, "CSRoot", "Runs", fault, realisation, x)
        )
        mocker.patch(
            "scripts.set_runparams.utils.load_sim_params", get_mocked_sim_params
        )
        mocker.patch(
            "scripts.set_runparams.shared.write_to_py",
            lambda x, y: mocked_write_to_py(
                os.path.join(
                    root_path,
                    "CSRoot",
                    "Runs",
                    fault,
                    realisation,
                    "LF",
                    generated_file_name,
                ),
                y,
            ),
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

        inputs = get_input_params(root_path, func_name, params)
        set_runparams.create_run_params(*inputs)
        output_file = os.path.join(
            root_path, "CSRoot", "Runs", fault, realisation, "LF", generated_file_name
        )
        benchmark_file = os.path.join(
            root_path, OUTPUT, "{}".format(generated_file_name)
        )

        assert filecmp.cmp(output_file, benchmark_file)
