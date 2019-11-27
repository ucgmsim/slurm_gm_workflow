import inspect
import io
import os

from shared_workflow import shared_template
from testing.test_common_set_up import (
    get_input_params,
    OUTPUT,
    get_bench_output,
    set_up,
)


def test_get_partition():
    assert shared_template.get_partition("maui") == "nesi_research"
    assert shared_template.get_partition("mahuika") == "large"


def test_convert_time_to_hours():
    assert shared_template.convert_time_to_hours("00:10:00") == 10 / 60.0
    assert shared_template.convert_time_to_hours("01:00:00") == 1


# def test_write_sl_script(set_up, mocker):
#     func_name = "write_sl_script"
#     func = shared_template.write_sl_script
#     params = inspect.getfullargspec(func).args
#     mocker.patch(
#         "shared_workflow.shared_template.recipe_dir",
#         os.path.join(
#             os.path.dirname(os.path.realpath(__file__)), "..", "..", "templates"
#         ),
#     )
#     variable_lines = [11, 12]
#     for root_path, realisation in set_up:
#         input_params = get_input_params(root_path, func_name, params)
#
#         slurm_script = io.StringIO("")
#         mocker.patch(
#             "shared_workflow.shared_template.write_file",
#             lambda _, parts: slurm_script.write("\n".join(parts)),
#         )
#
#         func(*input_params)
#         test_output = [
#             "{}\n".format(line) for line in slurm_script.getvalue().split("\n")
#         ]
#
#         bench_output = open(
#             os.path.join(root_path, OUTPUT, "write_sl_script.sl")
#         ).readlines()
#
#         assert len(bench_output) == len(test_output)
#         for test_line, bench_line in zip(
#             [x for i, x in enumerate(test_output) if i not in variable_lines],
#             [x for i, x in enumerate(bench_output) if i not in variable_lines],
#         ):
#             assert test_line == bench_line


def test_generate_command(set_up):
    func_name = "generate_command"
    func = shared_template.generate_command
    params = inspect.getfullargspec(func).args
    for root_path, realisation in set_up:
        input_params = get_input_params(root_path, func_name, params)
        test_output = func(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output


# def test_generate_context(set_up):
#     func_name = "generate_context"
#     func = shared_template.generate_context
#     params = inspect.getfullargspec(func).args
#     for root_path, realisation in set_up:
#         input_params = get_input_params(root_path, func_name, params)
#         input_params[0] = os.path.join(
#             os.path.dirname(os.path.realpath(__file__)), "..", "..", "templates"
#         )
#         test_output = func(*input_params)
#         bench_output = get_bench_output(root_path, func_name)
#         assert test_output == bench_output


def test_resolve_header(set_up):
    func_name = "resolve_header"
    func = shared_template.resolve_header
    params = inspect.getfullargspec(func).args
    variable_lines = [11, 12]
    for root_path, realisation in set_up:
        input_params = get_input_params(root_path, func_name, params)
        input_params[0] = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "..", "..", "templates"
        )
        test_output = func(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        for test_line, bench_line in zip(
            [
                x
                for i, x in enumerate(test_output.split("\n"))
                if i not in variable_lines
            ],
            [
                x
                for i, x in enumerate(bench_output.split("\n"))
                if i not in variable_lines
            ],
        ):
            assert test_line == bench_line
