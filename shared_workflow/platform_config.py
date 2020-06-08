from enum import Enum
from os.path import join, dirname, abspath

from qcore.config import determine_machine_config, get_machine_config, host
from qcore.constants import PLATFORM_CONFIG


WORKFLOW_DIR = abspath(join(dirname(__file__), ".."))


def determine_platform_config(hostname=determine_machine_config()[0]):
    if hostname == "maui" or hostname == "mahuika":
        hpc_platform = "nesi"
    elif hostname == "stampede2":
        hpc_platform = "tacc"
    elif hostname == "nurion":
        hpc_platform = "kisti"
    elif hostname == "local":
        hpc_platform = "local"
    else:
        raise ValueError("Unexpected host given")

    basename = f"platform_{hpc_platform}.json"

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
