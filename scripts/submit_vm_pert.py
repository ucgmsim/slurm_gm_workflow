"""
Libs for submitting Velocity Model Pertbation jobs
"""
from collections import OrderedDict
from logging import Logger
from pathlib import Path

import estimation.estimate_wct as est
from shared_workflow.platform_config import get_target_machine
from shared_workflow.shared_automated_workflow import submit_script_to_scheduler
from qcore import utils
from qcore.config import get_machine_config
from qcore import constants as const
from qcore.qclogging import get_basic_logger
from qcore import simulation_structure as sim_struct
from shared_workflow.shared import set_wct
from shared_workflow.platform_config import (
    HPC,
    platform_config,
    get_platform_specific_script,
    get_target_machine,
)

DEFAULT_CPUS = 4

# def estimate_wc:
def get_vm_pert_cores_and_wct(
    vm_params, ncpus, target_machine, logger: Logger = get_basic_logger()
):
    # scale up the run_time if its a re-run
    # there is not check-pointing currently
    est_core_hours, est_run_time = est.est_VM_PERT_chours_single(
        vm_params["nx"], vm_params["ny"], vm_params["nz"], ncpus
    )
    #    estimated_CH, estimated_run_time = est_VM_PERT_chours_single(vm_params['nx'], vm_params['ny'], vm_params['nz'] , ncpus)
    #    if retries > 0:
    # only scales up once, as it should never take more than that in normal situtation
    #        est_run_time = est_run_time * 2
    # re-scale core/wct
    # re-scale wct base on machine MAX_JOB_WCT
    machine_config = get_machine_config(hostname=target_machine)
    max_wct = machine_config["MAX_JOB_WCT"]
    cores_per_node = machine_config["cores_per_node"]

    while est_run_time > max_wct:
        est_run_time = est_run_time / 2
        ncpus = ncpus * 2
        if ncpus > cores_per_node:
            # raise warning if WCT is still larger than MAX_JOB_WCT.
            # assumes machine does not support mp calls above one nodes.
            # TODO: extend this when qcore.configs contains relative info
            logger.warning(
                "run_time:{est_run_time} is still larger than {target_machine}'s max_wct:{max_wct}. Using wct:{max_wct} and ncpus:{cores_per_node}"
            )
            est_run_time = max_wct
            ncpus = cores_per_node
            break

    est_wct = set_wct(est_run_time, ncpus, auto=True, logger=logger)
    return est_wct, ncpus


def submit_vm_pert_main(
    root_folder, run_name, sim_dir, logger: Logger = get_basic_logger()
):

    # load vm_params.yaml for estimation
    VM_PARAMS_YAML = str(
        Path(sim_struct.get_fault_VM_dir(root_folder, run_name)) / "vm_params.yaml"
    )
    vm_params = utils.load_yaml(VM_PARAMS_YAML)
    ncpus = DEFAULT_CPUS
    # get machine job will be submitted to
    target_machine = get_target_machine(const.ProcessType.VM_PERT).name
    est_wct, ncpus = get_vm_pert_cores_and_wct(vm_params, ncpus, target_machine, logger)

    submit_script_to_scheduler(
        get_platform_specific_script(
            # ProcessType
            const.ProcessType.VM_PERT,
            # arguments dictionary
            OrderedDict(
                {
                    "VM_PARAMS_YAML": VM_PARAMS_YAML,
                    "OUTPUT_DIR": sim_struct.get_fault_VM_dir(root_folder, run_name),
                    "SRF_PATH": sim_struct.get_srf_path(root_folder, run_name),
                    "MGMT_DB_LOC": root_folder,
                    "REL_NAME": run_name,
                }
            ),
            # scheduler arguments
            OrderedDict(
                {
                    # overwrites the default time in the script
                    "time": est_wct,
                    # overwrites the job-name for monitoring purpose
                    "job_name": f"vm_pert_{run_name}",
                    "ncpus": ncpus,
                    # TODO:update if qcore.config contains relative info
                    "ntasks": 1,
                }
            ),
        ),
        const.ProcessType.VM_PERT.value,
        sim_struct.get_mgmt_db_queue(root_folder),
        sim_dir=sim_dir,
        run_name=run_name,
        target_machine=get_target_machine(const.ProcessType.VM_PERT).name,
        logger=logger,
    )
