import inspect
import os
import glob
import pickle

from shared_workflow import install_shared
from testing.test_common_set_up import (
    INPUT,
    OUTPUT,
    set_up,
    get_bench_output,
    get_input_params,
)


def test_install_simulation(set_up):
    func_name = "install_simulation"
    params = inspect.getfullargspec(install_shared.install_simulation).args
    for content in set_up:
        for root_path in content:
            input_params = get_input_params(root_path, func_name, params)
            test_output = install_shared.install_simulation(*input_params)
            bench_output = get_bench_output(root_path, func_name)
            assert test_output == bench_output


def test_install_bb(set_up):
    func_name = "install_bb"
    params = inspect.getfullargspec(install_shared.install_bb).args
    for content in set_up:
        for root_path in content:
            input_params = get_input_params(root_path, func_name, params)
            test_output = install_shared.install_bb(*input_params)
            bench_output = get_bench_output(root_path, func_name)
            assert test_output == bench_output
