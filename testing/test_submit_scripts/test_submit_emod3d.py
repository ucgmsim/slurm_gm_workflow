import inspect
import os
import glob
import pickle

from qcore.config import host
from qcore.utils import load_sim_params as mocked_load_sim_params
from shared_workflow.shared import set_wct as mocked_set_wct
from shared_workflow.shared import resolve_header as mocked_resolve_header

from testing.test_common_set_up import (
    INPUT,
    OUTPUT,
    set_up,
    get_bench_output,
    get_input_params,
    get_fault_from_rel,
)

import scripts.submit_emod3d


def get_mocked_resolve_header(root_path, fault, realisation):
    return lambda account, n_tasks, wallclock_limit, job_name, version, memory, exe_time, job_description, partition=None, additional_lines="", template_path="slurm_header.cfg", target_host=host, mail="test@test.com": mocked_resolve_header(
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
    )


def test_main(set_up, mocker):
    """No return value. Just check that it runs without crashing"""
    function = "main"

    mocker.patch(
        "scripts.submit_emod3d.set_wct", lambda x, y, z: mocked_set_wct(x, y, True)
    )
    mocker.patch("scripts.submit_emod3d.confirm", lambda x: False)

    for root_path, realisation in set_up:
        fault = get_fault_from_rel(realisation)
        file_path = os.path.join(
            root_path, INPUT, "{}_{}_{}.P".format("submit_emod3d.py", function, "args")
        )
        with open(file_path, "rb") as load_file:
            args = pickle.load(load_file)

        # Fault will probably change on each set of data, so reset these every time
        mocker.patch(
            "scripts.submit_emod3d.utils.load_sim_params",
            lambda x: mocked_load_sim_params(
                os.path.join(root_path, "CSRoot", "Runs", fault, realisation, x)
            ),
        )

        mocker.patch(
            "scripts.submit_emod3d.resolve_header",
            get_mocked_resolve_header(root_path, fault, realisation),
        )

        scripts.submit_emod3d.main(args)


def test_write_sl_script(set_up, mocker):
    """The return value of write_sl_script is a filename that depends on the install location and current time and it
    is there fore not practical to test it with """
    func_name = "submit_emod3d.py_write_sl_script"
    params = inspect.getfullargspec(scripts.submit_emod3d.write_sl_script).args
    for root_path, realisation in set_up:
        fault = get_fault_from_rel(realisation)

        mocker.patch(
            "scripts.submit_emod3d.resolve_header",
            get_mocked_resolve_header(root_path, fault, realisation),
        )

        mocker.patch(
            "scripts.submit_emod3d.utils.load_sim_params",
            lambda x: mocked_load_sim_params(
                os.path.join(root_path, "CSRoot", "Runs", fault, realisation, x)
            ),
        )

        input_params = get_input_params(root_path, func_name, params)
        print(input_params)
        script_loaction = scripts.submit_emod3d.write_sl_script(*input_params)
        os.remove(script_loaction)
