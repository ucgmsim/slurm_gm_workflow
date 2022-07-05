import inspect
import io
import os

import pytest

from workflow.automation.lib import shared_template
from workflow.automation.tests.test_common_set_up import (
    get_input_params,
    get_bench_output,
    set_up,
)


def test_convert_time_to_hours():
    assert shared_template.convert_time_to_hours("00:10:00") == 10 / 60.0
    assert shared_template.convert_time_to_hours("01:00:00") == 1


@pytest.mark.usefixtures("init_scheduler")
def test_write_sl_script(set_up, mocker):
    func_name = "write_sl_script"
    func = shared_template.write_sl_script
    params = inspect.getfullargspec(func).args
    for root_path, realisation in set_up:
        input_params = get_input_params(root_path, func_name, params)

        input_params[4]["platform_specific_args"] = {
            "n_tasks": input_params[4]["n_tasks"]
        }
        del input_params[4]["n_tasks"]
        input_params[6]["run_command"] = "srun"

        slurm_script = io.StringIO("")
        mocker.patch(
            "workflow.automation.lib.shared_template.write_file",
            lambda _, parts: slurm_script.write("\n".join(parts)),
        )

        func(*input_params)
        test_output = [
            "{}\n".format(line) for line in slurm_script.getvalue().split("\n")
        ]

        # testing if the test_output is non-zero output. it doesn't check whether this .sl script makes sense (it will be covered by end-to-end test)
        assert len(test_output) > 0


def test_generate_command(set_up):
    func_name = "generate_command"
    func = shared_template.generate_command
    params = inspect.getfullargspec(func).args
    for root_path, realisation in set_up:
        input_params = get_input_params(root_path, func_name, params)
        test_output = func(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output


def test_generate_context(set_up):
    func_name = "generate_context"
    func = shared_template.generate_context
    params = inspect.getfullargspec(func).args
    for root_path, realisation in set_up:
        input_params = get_input_params(root_path, func_name, params)
        input_params[0] = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "..", "..", "templates"
        )
        test_output = func(*input_params)
        # Check test output exists, doing a line by line comparison was considered to be too
        # expensive when changes were made which required the test data to be updated
        assert test_output


def test_resolve_header(set_up):
    func_name = "resolve_header"
    func = shared_template.resolve_header
    params = inspect.getfullargspec(func).args
    bench_variable_lines = [11, 12]
    output_variable_lines = [9, 11, 12]
    for root_path, realisation in set_up:
        input_params = get_input_params(root_path, func_name, params)
        input_params[0] = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "..", "..", "templates"
        )
        input_params[8] = "nesi_header.cfg"
        input_params.append({"n_tasks": 40})
        test_output = func(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        for test_line, bench_line in zip(
            [
                x
                for i, x in enumerate(test_output.split("\n"))
                if i not in output_variable_lines
            ],
            [
                x
                for i, x in enumerate(bench_output.split("\n"))
                if i not in bench_variable_lines
            ],
        ):
            print(test_line, bench_line)
            assert test_line == bench_line
