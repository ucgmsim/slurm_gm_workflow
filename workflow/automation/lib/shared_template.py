import os
from datetime import datetime

from jinja2 import Environment, FileSystemLoader

import qcore.constants as const
from qcore.utils import load_sim_params
from workflow.automation.lib.schedulers.scheduler_factory import Scheduler
from workflow.automation.platform_config import platform_config
from qcore import config

def write_sl_script(
    write_directory,
    sim_dir,
    process: const.ProcessType,
    script_prefix,
    header_dict,
    body_template_params,
    command_template_parameters,
    add_args={},
):
    params = load_sim_params(os.path.join(sim_dir, "sim_params.yaml"))
    common_header_dict = {
        "template_dir": platform_config[
            const.PLATFORM_CONFIG.SCHEDULER_TEMPLATES_DIR.name
        ],
        "memory": platform_config[const.PLATFORM_CONFIG.DEFAULT_MEMORY.name],
        "exe_time": const.timestamp,
        "version": platform_config[const.PLATFORM_CONFIG.SCHEDULER.name],
        "write_directory": write_directory,
    }
    common_template_params = {
        "sim_dir": sim_dir,
        "srf_name": os.path.splitext(os.path.basename(params["srf_file"]))[0],
        "mgmt_db_location": params["mgmt_db_location"],
        "submit_command": generate_command(
            process,
            sim_dir,
            process.command_template,
            command_template_parameters,
            add_args,
        ),
    }

    common_header_dict.update(header_dict)
    header = resolve_header(**common_header_dict)

    (template_name, template_params) = body_template_params
    common_template_params.update(template_params)
    body = generate_context(
        platform_config[const.PLATFORM_CONFIG.SCHEDULER_TEMPLATES_DIR.name],
        template_name,
        common_template_params,
    )

    script_name = os.path.abspath(
        os.path.join(
            write_directory,
            "{}_{}.{}".format(
                script_prefix,
                datetime.now().strftime(const.TIMESTAMP_FORMAT),
                Scheduler.get_scheduler().SCRIPT_EXTENSION,
            ),
        )
    )

    content = "\n".join([header, body])
    write_to_file(content, script_name)

    return script_name


def write_to_file(content, script_name):
    with open(script_name, "w") as f:
        f.write(content)


def generate_command(
    process: const.ProcessType,
    sim_dir,
    command_template,
    template_parameters,
    add_args={},
):
    command_parts = []

    if process.uses_acc:
        acc_dir = os.path.join(sim_dir, process.str_value, "Acc")
        command_parts.append("mkdir -p {}\n".format(acc_dir))

    command_parts.append(command_template.format(**template_parameters))

    for key in add_args:
        if (
            add_args[key] is False
        ):  # Don't add store_true type arg at all if False.(This repo has no use of store_false)
            continue
        command_parts.append("--" + key)
        if add_args[key] is True:  # store_true type arg needs no value
            continue
        command_parts.append(str(add_args[key]))

    return " ".join(list(map(str, command_parts)))


def generate_context(simulation_dir, template_path, parameter_dict):
    """
    return the template context for submission script
    :param simulation_dir:
    :param template_path:
    :param parameter_dict:
    :return:
    """
    j2_env = Environment(loader=FileSystemLoader(simulation_dir), trim_blocks=True)
    context = j2_env.get_template(template_path).render(**parameter_dict)
    return context


def resolve_header(
    template_dir,
    wallclock_limit,
    job_name,
    version,
    memory,
    exe_time,
    job_description,
    additional_lines="",
    template_path=None,
    write_directory=".",
    platform_specific_args={},
):
    if template_path is None:
        template_path = platform_config[const.PLATFORM_CONFIG.HEADER_FILE.name]

    j2_env = Environment(loader=FileSystemLoader(template_dir), trim_blocks=True)

    wallclock_limit = min(wallclock_limit, str(config.qconfig[config.ConfigKeys.MAX_JOB_WCT.name]))

    header = j2_env.get_template(template_path).render(
        version=version,
        job_description=job_description,
        job_name=job_name,
        wallclock_limit=wallclock_limit,
        memory=memory,
        additional_lines=additional_lines,
        exe_time=exe_time,
        write_dir=write_directory,
        **platform_specific_args,
    )
    return header


def convert_time_to_hours(time_str):
    h, m, s = time_str.split(":")
    return int(h) + int(m) / 60.0 + int(s) / 3600.0
