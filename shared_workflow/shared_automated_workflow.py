"""
Shared functions only used by the automated workflow
"""
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime
from logging import Logger
from subprocess import Popen, PIPE
from typing import List

import qcore.constants as const
from qcore.config import host
from scripts.management.MgmtDB import MgmtDB
from qcore.qclogging import get_basic_logger, NOPRINTCRITICAL


def get_queued_tasks(user=None, machine=const.HPC.maui):
    if user is not None:
        cmd = "squeue -A {} -o '%A %t' -h -M {} -u {}".format(
            const.DEFAULT_ACCOUNT, machine.value, user
        )
    else:
        cmd = "squeue -A {} -o '%A %t' -h -M {}".format(
            const.DEFAULT_ACCOUNT, machine.value
        )

    process = Popen(shlex.split(cmd), stdout=PIPE, encoding="utf-8")
    (output, err) = process.communicate()
    process.wait()
    try:
        header = output.split("\n")[1]
    except:
        raise EnvironmentError(
            "squeue did not return expected output. Ignoring for this iteration."
        )
    else:
        if header != "JOBID ST":
            raise EnvironmentError(
                "squeue did not return expected output. Ignoring for this iteration."
            )

    output_list = list(filter(None, output.split("\n")[1:]))
    return output_list


def submit_sl_script(
    script: str,
    proc_type: int,
    queue_folder: str,
    run_name: str,
    submit_yes: bool = False,
    target_machine: str = None,
    logger: Logger = get_basic_logger(),
):
    """Submits the slurm script and updates the management db"""
    if submit_yes:
        logger.debug("Submitting {} on machine {}".format(script, target_machine))
        if target_machine and target_machine != host:
            res = exe(
                "sbatch --export=CUR_ENV,CUR_HPC -M {} {}".format(
                    target_machine, script
                ),
                debug=False,
            )
        else:
            res = exe("sbatch {}".format(script), debug=False)
        if len(res[1]) == 0:
            logger.debug("Successfully submitted task to slurm")
            # no errors, return the job id
            return_words = res[0].split()
            job_index = return_words.index("job")
            jobid = return_words[job_index + 1]
            try:
                int(jobid)
            except ValueError:
                logger.critical(
                    "{} is not a valid jobid. Submitting the "
                    "job most likely failed. The return message was {}".format(
                        jobid, res[0]
                    )
                )
                sys.exit()

            add_to_queue(
                queue_folder,
                run_name,
                proc_type,
                const.Status.queued.value,
                job_id=jobid,
                logger=logger,
            )
            return jobid
        else:
            logger.error("An error occurred during job submission: {}".format(res[1]))
    else:
        logger.info("User chose to submit the job manually")


def add_to_queue(
    queue_folder: str,
    run_name: str,
    proc_type: int,
    status: int,
    job_id: int = None,
    error: str = None,
    logger: Logger = get_basic_logger(),
):
    """Adds an update entry to the queue"""
    logger.debug(
        "Adding task to the queue. Realisation: {}, process type: {}, status: {}, job_id: {}, error: {}".format(
            run_name, proc_type, status, job_id, error
        )
    )
    filename = os.path.join(
        queue_folder,
        "{}.{}.{}".format(
            datetime.now().strftime(const.QUEUE_DATE_FORMAT), run_name, proc_type
        ),
    )

    if os.path.exists(filename):
        logger.log(
            NOPRINTCRITICAL,
            "An update with the name {} already exists. This should never happen. Quitting!".format(
                os.path.basename(filename)
            ),
        )
        raise Exception(
            "An update with the name {} already exists. This should never happen. Quitting!".format(
                os.path.basename(filename)
            )
        )

    logger.debug("Writing update file to {}".format(filename))

    with open(filename, "w") as f:
        json.dump(
            {
                MgmtDB.col_run_name: run_name,
                MgmtDB.col_proc_type: proc_type,
                MgmtDB.col_status: status,
                MgmtDB.col_job_id: job_id,
                "error": error,
            },
            f,
        )

    if not os.path.isfile(filename):
        logger.critical("File {} did not successfully write".format(filename))
    else:
        logger.debug("Successfully wrote task update file")


def exe(
    cmd,
    debug=True,
    shell=False,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    non_blocking=False,
):
    """cmd is either a str or a list. but it will be processed as a list.
    this is to accommodate the default shell=False. (for security reason)
    If we wish to support a simple shell command like "echo hello"
    without switching on shell=True, cmd should be given as a list.

    If non_blocking is set, then the Popen instance is returned instead of the
    output and error.
    """
    if type(cmd) == str:
        cmd = cmd.split(" ")

    if debug:
        print(" ".join(cmd))

    p = subprocess.Popen(
        cmd, shell=shell, stdout=stdout, stderr=stderr, encoding="utf-8"
    )
    if non_blocking:
        return p

    out, err = p.communicate()
    if debug:
        if out:
            print(out)
        if err:
            print(err, file=sys.stderr)
            print(err)  # also printing to stdout (syncing err msg to cmd executed)

    return out, err


def check_mgmt_queue(
    queue_entries: List[str], run_name: str, proc_type: int, logger=get_basic_logger()
):
    """Returns True if there are any queued entries for this run_name and process type,
    otherwise returns False.
    """
    logger.debug(
        "Checking to see if the realisation {} has a process of type {} in updates folder".format(
            run_name, proc_type
        )
    )
    for entry in queue_entries:
        logger.debug("Checking against {}".format(entry))
        _, entry_run_name, entry_proc_type = entry.split(".")
        if entry_run_name == run_name and entry_proc_type == str(proc_type):
            logger.debug("It's a match, returning True")
            return True
    logger.debug("No match found")
    return False
