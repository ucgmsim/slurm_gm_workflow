import inspect

from scripts.common import shared
from tests.unit.test_common_set_up import get_input_params, get_bench_output


# test for install_simualtion inside install_cybershake_fault.py
def test_get_stations(set_up):
    func_name = "get_stations"
    params = inspect.getfullargspec(shared.get_stations).args
    for root_path, _ in set_up:
        input_params = get_input_params(root_path, func_name, params)
        test_output = shared.get_stations(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output


def test_user_select(set_up, mocker):
    func_name = "user_select"
    params = inspect.getfullargspec(shared.user_select).args
    for root_path, realisation in set_up:
        input_params = get_input_params(root_path, func_name, params)
        mocker.patch("shared_workflow.shared.input", lambda x: "2")
        test_output = shared.user_select(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output
