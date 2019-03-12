import inspect
import os
import glob
import pickle

from qcore.config import host
from qcore.utils import load_sim_params as load_sp
from shared_workflow.shared import set_wct as set_wall_clock
from shared_workflow.shared import resolve_header as mocked_resolve_header

from testing.test_common_set_up import (
    INPUT,
    OUTPUT,
    set_up,
    get_bench_output,
    get_input_params,
    get_fault_from_rel,
)

import scripts.submit_hf


def test_main(set_up, mocker):
    """No return value. Just check that it runs without crashing"""
    function = "main"
    for root_path, realisation in set_up:
        fault = get_fault_from_rel(realisation)
        file_path = os.path.join(
            root_path, INPUT, "{}_{}_{}.P".format("submit_hf.py", function, "args")
        )
        with open(file_path, "rb") as load_file:
            args = pickle.load(load_file)

        mocker.patch("scripts.submit_hf.set_wct", lambda x, y, z: set_wall_clock(x, y, True))

        mocker.patch("scripts.submit_hf.confirm", lambda x: False)

        mocker.patch(
            "scripts.submit_hf.utils.load_sim_params",
            lambda x: load_sp(
                os.path.join(root_path, "CSRoot", "Runs", fault, realisation, x)
            ),
        )

        mocker.patch(
            "scripts.submit_hf.resolve_header",
            lambda account, n_tasks, wallclock_limit, job_name, version, memory, exe_time, job_description, partition=None, additional_lines="", template_path="slurm_header.cfg", target_host=host, mail="test@test.com",: mocked_resolve_header(
                account,
                n_tasks,
                wallclock_limit,
                job_name,
                version,
                memory,
                exe_time,
                job_description,
                partition=partition,
                additional_lines=additional_lines,
                template_path=os.path.join(
                    os.path.basename(root_path),
                    "CSRoot",
                    "Runs",
                    fault,
                    realisation,
                    template_path,
                ),
                target_host=target_host,
                mail=mail,
            ),
        )

        scripts.submit_hf.main(args)

