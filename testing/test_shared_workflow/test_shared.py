import inspect
import io
import os

from shared_workflow import shared
from testing.test_common_set_up import (
    INPUT,
    OUTPUT,
    set_up,
    get_input_params,
    get_bench_output,
)


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


def test_get_partition():
    assert shared.get_partition("maui") == "nesi_research"
    assert shared.get_partition("mahuika") == "large"


def test_convert_time_to_hours():
    assert shared.convert_time_to_hours("00:10:00") == 10 / 60.0
    assert shared.convert_time_to_hours("01:00:00") == 1


def test_write_sl_script(set_up, mocker):
    func_name = "write_sl_script"
    func = shared.write_sl_script
    params = inspect.getfullargspec(func).args

    variable_lines = [11, 12]
    for root_path, realisation in set_up:
        input_params = get_input_params(root_path, func_name, params)

        slurm_script = io.StringIO("")
        mocker.patch(
            "shared_workflow.shared.write_file",
            lambda _, parts: slurm_script.write("\n".join(parts)),
        )

        func(*input_params)
        test_output = [
            "{}\n".format(line) for line in slurm_script.getvalue().split("\n")
        ]

        bench_output = open(
            os.path.join(root_path, OUTPUT, "write_sl_script.sl")
        ).readlines()

        #assert len(bench_output) == len(test_output)
        for test_line, bench_line in zip(test_output, bench_output):
            assert test_line == bench_line


def test_generate_command(set_up):
    func_name = "generate_command"
    func = shared.generate_command
    params = inspect.getfullargspec(func).args
    for root_path, realisation in set_up:
        input_params = get_input_params(root_path, func_name, params)
        test_output = func(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output


def test_generate_context(set_up):
    func_name = "generate_context"
    func = shared.generate_context
    params = inspect.getfullargspec(func).args
    for root_path, realisation in set_up:
        input_params = get_input_params(root_path, func_name, params)
        test_output = func(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output


def test_resolve_header(set_up):
    func_name = "resolve_header"
    func = shared.resolve_header
    params = inspect.getfullargspec(func).args
    for root_path, realisation in set_up:
        input_params = get_input_params(root_path, func_name, params)
        test_output = func(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        for test_line, bench_line in zip(test_output.split(), bench_output.split()):
            assert test_line == bench_line
