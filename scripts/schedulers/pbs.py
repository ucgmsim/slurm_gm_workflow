import os
from logging import Logger
from typing import List

from qcore.constants import timestamp

from scripts.management.MgmtDB import SlurmTask
from scripts.schedulers.abstractscheduler import AbstractScheduler


class Pbs(AbstractScheduler):
    def get_metadata(self, db_running_task: SlurmTask, task_logger: Logger):
        pass

    HEADER_TEMPLATE = "pbs_header.cfg"
    STATUS_DICT = {"R": 3, "Q": 2, "E": 3, "F": 4}
    SCRIPT_EXTENSION = "pbs"

    def submit_job(
        self, sim_dir, script_location: str, target_machine: str = None
    ) -> int:
        self.logger.debug(
            "Submitting {} on machine {}".format(script_location, target_machine)
        )

        if target_machine and target_machine != self.current_machine:
            raise self.raise_exception(
                "Job submission for different machine is not supported",
                NotImplementedError,
            )

        cwd = os.getcwd()
        os.chdir(sim_dir)  # KISTI doesn't allow job submission from home
        out, err = self._run_command_and_wait(
            f"qsub -q {self.account} {script_location}"
        )
        os.chdir(cwd)
        self.logger.debug((out, err))

        if len(err) != 0:
            raise self.raise_exception(
                f"An error occurred during job submission: {err}"
            )

        self.logger.debug("Successfully submitted task to slurm")
        # no errors, return the job id
        return_words = out.split(".pbs")  # 4027812.pbs
        self.logger.debug(return_words)

        try:
            jobid = int(return_words[0])
        except ValueError:
            raise self.raise_exception(
                f"{return_words[0]} is not a valid jobid. Submitting the job most likely failed. The return message was {out}"
            )

        out, err = self._run_command_and_wait(f"qstat {jobid}")
        try:
            job_name = out.split(" ")[1]
        except Exception:
            raise self.raise_exception(
                "Unable to determine job name from qstat. Exiting"
            )

        self.logger.debug(f"Return from qstat, stdout: {out}, stderr:{err}")

        f_name = f"{job_name}_{timestamp}_{jobid}"
        # Set the error and output logs to <name>_<time>_<job_id> as this cannot be done before submission time
        self._run_command_and_wait(f"qalter -o {f_name}.out {jobid}")
        self._run_command_and_wait(f"qalter -e {f_name}.err {jobid}")
        return jobid

    def cancel_job(self, job_id: int, target_machine=None) -> None:
        self._run_command_and_wait(cmd=[f"qdel {job_id}"], shell=True)

    def check_queues(self, user: str = None, target_machine=None) -> List[str]:
        if (
            user is not None
        ):  # just print the list of jobid and status (a space between)
            if user is True:
                user = self.user_name
            cmd = ["qstat", "-u", f"{user}"]
            header_pattern = "pbs:"
            header_idx = 1
            job_list_idx = 5
        else:
            cmd = "qstat "
            header_pattern = "Job id"
            header_idx = 0
            job_list_idx = 3

        (output, err) = self._run_command_and_wait(cmd.split(" "), encoding="utf-8")

        try:
            header = output.split("\n")[header_idx]
        except Exception:
            if user is not None and len(output) == 0:  # empty queue has no header
                return []
            raise EnvironmentError(
                f"qstat did not return expected output. Ignoring for this iteration. Actual output: {output}"
            )
        else:
            if header_pattern not in header:
                raise EnvironmentError(
                    f"qstat did not return expected output. Ignoring for this iteration. Actual output: {output}"
                )
        # only keep the relevant info
        jobs = []
        for l in [
            line.split() for line in output.split("\n")[job_list_idx:-1]
        ]:  # last line is empty
            self.logger.debug(l)
            jobs.append("{} {}".format(l[0].split(".")[0], l[-2]))

        output_list = list(filter(None, jobs))
        self.logger.debug(output_list)
        return output_list

    @staticmethod
    def process_arguments(script_path: str, arguments: List[str]):
        return f"-V {' '.join(arguments)} {script_path} "
