import inspect
import os
import glob
import pickle

from shared_workflow import install_shared
from testing.test_common_set_up import INPUT, OUTPUT, set_up


def test_install_simulation(set_up):
    func_name = 'install_simulation'
    params = inspect.getfullargspec(install_shared.install_simulation).args
    for content in set_up:
        for root_path in content:
            input_params = []
            for param in params:
                with open(
                        os.path.join(root_path, INPUT, func_name + "_{}.P".format(param)), "rb"
                ) as load_file:
                    input_param = pickle.load(load_file)
                    input_params.append(input_param)
            test_output = install_shared.install_simulation(*params)

            output_dir = os.path.join(root_path, OUTPUT)
            bench_output = []

            for fname in glob.glob1(output_dir, '{}_*.P'.format(func_name)):
                with open(os.path.join(output_dir, fname), 'rb') as load_file:
                    bench = pickle.load(load_file)
                    bench_output.append(bench)


