import inspect
import os
import glob
import pickle
import shutil

from shared_workflow import install_shared
from testing.test_common_set_up import (
    set_up,
    get_bench_output,
    get_input_params,
    REALISATIONS
)


def test_install_simulation(set_up):
    func_name = "install_simulation"
    params = inspect.getfullargspec(install_shared.install_simulation).args
    for root_path, _ in set_up:
        input_params = get_input_params(root_path, func_name, params)
        test_output = install_shared.install_simulation(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output


def teardown_module(module):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    test_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
    for realization, _ in REALISATIONS:
        fault = realization.split('_HYP')[0]
        fault_install_dir = os.path.join(test_dir, fault)
        if os.path.isdir(fault_install_dir):
            shutil.rmtree(fault_install_dir)
        else:
            fault_install_dir = os.path.join(current_dir, fault)
            shutil.rmtree(fault_install_dir)