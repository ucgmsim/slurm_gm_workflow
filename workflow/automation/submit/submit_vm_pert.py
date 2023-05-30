"""
Libs for submitting Velocity Model Pertbation jobs
"""
from collections import OrderedDict
from logging import Logger
from pathlib import Path

import workflow.automation.estimation.estimate_wct as est
from workflow.automation.estimation import estimate_wct
from workflow.automation.lib.shared_automated_workflow import submit_script_to_scheduler
from qcore import utils
from qcore import constants as const
from qcore.qclogging import get_basic_logger
from qcore import simulation_structure as sim_struct
from workflow.automation import platform_config

VM_PERT_TARGET_MACHINE = platform_config.get_target_machine(
    const.ProcessType.VM_PERT
).name
DEFAULT_CPUS = platform_config.platform_config[
    const.PLATFORM_CONFIG.VM_PERT_DEFAULT_NCORES.name
]


def get_vm_pert_cores_and_wct(vm_params, ncpus, logger: Logger = get_basic_logger()):
    est_core_hours, est_run_time = est.est_VM_PERT_chours_single(
        vm_params["nx"], vm_params["ny"], vm_params["nz"], ncpus
    )

    ncpus, wct = estimate_wct.confine_wct_node_parameters(
        ncpus,
        est_run_time,
        hyperthreaded=const.ProcessType.VM_PERT.is_hyperth,
        can_checkpoint=False,  # hard coded for now as this is not available programatically
        preserve_core_count=True,
        logger=logger,
    )
    wct_string = estimate_wct.convert_to_wct(wct)
    return wct_string, ncpus


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
    est_wct, ncpus = get_vm_pert_cores_and_wct(vm_params, ncpus, logger)

    submit_script_to_scheduler(
        platform_config.get_platform_specific_script(
            # ProcessType
            const.ProcessType.VM_PERT,
            # arguments dictionary
            OrderedDict(
                {
                    "VM_PARAMS_YAML": VM_PARAMS_YAML,
                    "OUTPUT_DIR": sim_struct.get_fault_VM_dir(root_folder, run_name),
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
                    "nodes": 1,
                    # TODO:update if qcore.config contains relative info
                    "ntasks": 1,
                }
            ),
        ),
        const.ProcessType.VM_PERT.value,
        sim_struct.get_mgmt_db_queue(root_folder),
        sim_dir=sim_dir,
        run_name=run_name,
        target_machine=VM_PERT_TARGET_MACHINE,
        logger=logger,
    )
