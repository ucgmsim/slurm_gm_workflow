# TODO: move this file to qcore library ASAP


def resolve_header(account, nb_cpus, wallclock_limit, job_name, version, memory, exe_time , job_description,  additional_lines="", cfg='slurm_header.cfg'):
    with open(cfg) as f:
        lines = f.readlines()
        full_txt = "".join(lines)
        full_txt = full_txt.replace("{{account}}", account)
        full_txt = full_txt.replace("{{job_name}}", job_name)
        full_txt = full_txt.replace("{{nb_cpus}}", nb_cpus)
        full_txt = full_txt.replace("{{wallclock_limit}}", wallclock_limit)
        full_txt = full_txt.replace("{{version}}", version)
        full_txt = full_txt.replace("{{memory}}", memory)
        full_txt = full_txt.replace("{{job_description}}", job_description)
        full_txt = full_txt.replace("{{mail}}", "test@test.com")
        full_txt = full_txt.replace("{{additional_lines}}", additional_lines)
        full_txt = full_txt.replace("{{exe_time}}",exe_time)

    return full_txt
