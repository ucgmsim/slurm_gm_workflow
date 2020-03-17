import inspect
import os

from qcore.utils import load_yaml as mocked_load_yaml

from shared_workflow import install_shared
from testing.test_common_set_up import set_up, get_bench_output, get_input_params


def test_install_simulation(set_up, mocker):
    func_name = "install_simulation"
    params = inspect.getfullargspec(install_shared.install_simulation).args
    for root_path, _ in set_up:
        input_params = get_input_params(root_path, func_name, params)

        mocker.patch(
            "shared_workflow.install_shared.defaults.workflow_root",
            "{}/AdditionalData".format(root_path),
        )
        mocker.patch(
            "shared_workflow.install_shared.defaults.bin_process_dir",
            "/home/root/git/slurm_gm_workflow/scripts",
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
                    "root_defaults.yaml",
                )
            )
            if "root_defaults.yaml" in x
            else mocked_load_yaml(x),
        )
        mocker.patch(
            "shared_workflow.install_shared.defaults.recipe_dir",
            os.path.join(root_path, "..", "..", "templates"),
        )

        for i in range(len(input_params)):
            if isinstance(input_params[i], str) and input_params[i].startswith(
                ("CSRoot", "AdditionalData", "PangopangoF29/")
            ):
                input_params[i] = os.path.join(root_path, input_params[i])

        test_output = install_shared.install_simulation(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        print(test_output)
        print(bench_output)
        assert test_output[0] == bench_output[0]
