import os
import pytest
import pickle

from qcore import utils

INPUT = "input"
OUTPUT = "output"
REALISATIONS = [
    (
        "PangopangoF29_HYP01-10_S1244",
        "http://ec2-54-206-55-199.ap-southeast-2.compute.amazonaws.com/static/public/testing/slurm_gm_workflow/PangopangoF29_HYP01-10_S1244.zip",
    )
]


def get_input_params(root_path, func_name, params):
    input_params = []
    for param in params:
        print(os.path.join(root_path, INPUT, func_name + "_{}.P".format(param)))
        with open(
                os.path.join(root_path, INPUT, func_name + "_{}.P".format(param)), "rb"
        ) as load_file:
            input_param = pickle.load(load_file)
            input_params.append(input_param)
    return input_params


def get_bench_output(root_path, func_name):
    with open(
            os.path.join(root_path, OUTPUT, func_name + "_ret_val.P"), "rb"
    ) as load_file:
        bench_output = pickle.load(load_file)
    return bench_output


@pytest.yield_fixture(scope="session", autouse=True)
def set_up(request):
    return utils.test_set_up(REALISATIONS)
