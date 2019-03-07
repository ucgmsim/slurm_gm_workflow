import os
import pickle
import inspect

from shared_workflow import shared
from testing.test_common_set_up import INPUT, OUTPUT, set_up, get_input_params, get_bench_output


def test_get_stations(set_up):
    func_name = 'get_stations'
    params = inspect.getfullargspec(shared.get_stations).args
    for content in set_up:
        for root_path in content:
            input_params = get_input_params(root_path, func_name, params)
            test_output = shared.get_stations(*input_params)
            bench_output = get_bench_output(root_path, func_name)
            assert test_output == bench_output


def test_user_select(set_up):
    func_name = 'user_select'
    params = inspect.getfullargspec(shared.get_stations).args
    for content in set_up:
        for root_path in content:
            input_params = get_input_params(root_path, func_name, params)
            test_output = shared.user_select(*input_params)
            bench_output = get_bench_output(root_path, func_name)
            assert test_output == bench_output

