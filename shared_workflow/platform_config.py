from enum import Enum, auto
from os.path import join, dirname, abspath
from typing import Dict, Union

from numpy.ma import ceil

from qcore.config import (
    determine_machine_config,
    get_machine_config,
    host,
    qconfig,
    __KnownMachines,
)
from qcore.constants import PLATFORM_CONFIG, ProcessType

WORKFLOW_DIR = abspath(join(dirname(__file__), ".."))


class Platforms(Enum):
    LOCAL = auto()
    NESI = auto()
    TACC = auto()
    KISTI = auto()


def determine_platform_config(hostname=determine_machine_config()[0]):
    """
    Determines the platform the script is running on
    :param hostname: The name of the machine currently being run
    :return: The platform being run, the path to the config for that platform
    """
    if (
        hostname == __KnownMachines.maui.name
        or hostname == __KnownMachines.mahuika.name
    ):
        hpc_platform = Platforms.NESI
    elif hostname == __KnownMachines.stampede2.name:
        hpc_platform = Platforms.TACC
    elif hostname == __KnownMachines.nurion.name:
        hpc_platform = Platforms.KISTI
    elif hostname == __KnownMachines.local.name:
        hpc_platform = Platforms.LOCAL
    else:
        raise ValueError("Unexpected host given")

    basename = f"platform_{hpc_platform.name.lower()}.json"

    config_path = join(dirname(abspath(__file__)), "platform_configs", basename)
    return hpc_platform, config_path


platform, platform_config_path = determine_platform_config(host)
platform_config = get_machine_config(config_path=platform_config_path)

# Allows for loading data files from the templates and models directories
for key, value in platform_config.items():
    if isinstance(value, str) and "$workflow" in value:
        platform_config[key] = value.replace("$workflow", WORKFLOW_DIR)

errors = set(platform_config.keys()).symmetric_difference(
    set([key.name for key in PLATFORM_CONFIG])
)
if errors:
    missing_keys = []
    extra_keys = []
    for key in errors:
        if key in platform_config:
            extra_keys.append(key)
        else:
            missing_keys.append(key)
    message = (
        f"There were some errors with the platform config file {platform_config_path}."
    )
    if missing_keys:
        message += f" Missing keys: {', '.join(missing_keys)}."
    if extra_keys:
        message += f" Additional keys found: {', '.join(extra_keys)}."
    raise ValueError(message)

# Dynamically generate the HPC enum

HPC = Enum(
    "HPC", platform_config[PLATFORM_CONFIG.AVAILABLE_MACHINES.name], module=__name__
)


def get_target_machine(process: Union[ProcessType, str, int]) -> HPC:
    """
    Takes in a process and returns the machine that task is expected to run on
    :param process: The process to be checked. Represented by either the name index or ProcessType enum
    :return: The HPC the task is to run on
    """
    if isinstance(process, str):
        process = ProcessType[process]
    elif isinstance(process, int):
        process = ProcessType(process)
    return HPC[platform_config[PLATFORM_CONFIG.MACHINE_TASKS.name][process.name]]


def get_platform_specific_script(
    process: ProcessType, arguments: Dict[str, str]
) -> str:
    """
    Returns the path to the script with arguments correctly formatted for the scheduler
    :param process: The process to get the script for
    :param arguments: Any arguments to be passed to the script
    :return: The string representing the path to the script with the appropriate arguments to run it
    """

    # To prevent circular dependency
    from scripts.schedulers.scheduler_factory import Scheduler

    scheduler = Scheduler.get_scheduler()

    platform_dir = f"{platform.name.lower()}_scripts"
    script_extension = scheduler.SCRIPT_EXTENSION
    script_name = {
        ProcessType.rrup: "calc_rrups_single",
        ProcessType.clean_up: "clean_up",
        ProcessType.HF2BB: "hf2bb",
        ProcessType.LF2BB: "lf2bb",
        ProcessType.plot_srf: "plot_srf",
        ProcessType.plot_ts: "plot_ts",
    }[process]

    return scheduler.process_arguments(
        join(
            WORKFLOW_DIR, "scripts", platform_dir, f"{script_name}.{script_extension}"
        ),
        arguments,
    )


def get_platform_node_requirements(task_count):
    """
    Generates the number of tasks, nodes and tasks per node for each platform as required by the header for that platform
    :param task_count: The number of cores/threads/tasks the job will take
    :return: A dictionary containing the values to be used in the header
    """
    if platform == Platforms.NESI or platform == Platforms.LOCAL:
        return {"n_tasks": task_count}
    elif platform == Platforms.TACC:
        return {
            "n_tasks": task_count,
            "n_nodes": int(ceil(task_count / qconfig["cores_per_node"])),
        }
    elif platform == Platforms.KISTI:
        n_nodes = int(ceil(task_count / qconfig["cores_per_node"]))
        return {"n_nodes": n_nodes, "n_tasks_per_node": qconfig["cores_per_node"]}
    raise NotImplementedError(
        f"The platform {platform}  does not have related node requirements"
    )
