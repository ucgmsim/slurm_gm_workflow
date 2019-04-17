import os
from datetime import datetime

from jinja2 import Environment, FileSystemLoader
from qcore import constants as const
from qcore.config import host
from qcore.utils import load_sim_params
from shared_workflow.shared import write_file
from shared_workflow.shared_defaults import recipe_dir


def write_sl_script(
    write_directory,
    sim_dir,
    process: const.ProcessType,
    script_prefix,
    header_dict,
    body_template_params,
    command_template_parameters,
    cmd_args,
    add_args={},
):
    params = load_sim_params(os.path.join(sim_dir, "sim_params.yaml"))
    common_header_dict = {
        "template_dir": recipe_dir,
        "memory": const.DEFAULT_MEMORY,
        "exe_time": const.timestamp,
        "version": "slurm",
        "account": cmd_args.account,
        "target_host": cmd_args.machine,
        "write_directory": write_directory,
    }
    common_template_params = {
        "sim_dir": sim_dir,
        "srf_name": os.path.splitext(os.path.basename(params.srf_file))[0],
        "mgmt_db_location": params.mgmt_db_location,
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
    body = generate_context(recipe_dir, template_name, common_template_params)

    script_name = os.path.abspath(
        os.path.join(
            write_directory,
            "{}_{}.sl".format(
                script_prefix, datetime.now().strftime(const.TIMESTAMP_FORMAT)
            ),
        )
    )
    write_file(script_name, [header, body])

    return script_name


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
        command_parts.append("--" + key)
        if add_args[key] is True:
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
    account,
    n_tasks,
    wallclock_limit,
    job_name,
    version,
    memory,
    exe_time,
    job_description,
    partition=None,
    additional_lines="",
    template_path="slurm_header.cfg",
    target_host=host,
    mail="test@test.com",
    write_directory=".",
):
    if partition is None:
        partition = get_partition(target_host, convert_time_to_hours(wallclock_limit))

    j2_env = Environment(loader=FileSystemLoader(template_dir), trim_blocks=True)
    header = j2_env.get_template(template_path).render(
        version=version,
        job_description=job_description,
        job_name=job_name,
        account=account,
        n_tasks=n_tasks,
        wallclock_limit=wallclock_limit,
        mail=mail,
        memory=memory,
        additional_lines=additional_lines,
        exe_time=exe_time,
        partition=partition,
        write_dir=write_directory,
    )
    return header


def get_partition(machine, core_hours=None):
    if machine == const.HPC.maui.value:
        partition = "nesi_research"
    elif machine == const.HPC.mahuika.value:
        if core_hours and core_hours < 6:
            partition = "large"
        else:
            partition = "large"
    else:
        partition = ""
    return partition


def convert_time_to_hours(time_str):
    h, m, s = time_str.split(":")
    return int(h) + int(m) / 60.0 + int(s) / 3600.0
