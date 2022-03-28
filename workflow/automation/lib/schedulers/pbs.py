import json
import os
from logging import Logger
from typing import List, Dict
from datetime import timedelta

from qcore.constants import timestamp

from workflow.automation.lib.MgmtDB import SchedulerTask
from workflow.automation.lib.schedulers.abstractscheduler import AbstractScheduler


class Pbs(AbstractScheduler):
    def get_metadata(self, db_running_task: SchedulerTask, task_logger: Logger):
        """
        Queries qstat for the information of a completed task
        :param db_running_task: The task to retrieve metadata for
        :param task_logger: the logger for the task
        :return: A tuple containing the expected metadata
        """
        cmd = [f"qstat -f -F json -x {db_running_task.job_id}"]

        out, err = self._run_command_and_wait(cmd, shell=True)
        # remove values that contains backslash
        # several GPU-related variables contains only a single backslash and nothing else, which will cause loads() to crash
        out = out.replace("\\", "")
        json_dict = json.loads(out, strict=False)

        if "Jobs" not in json_dict:
            # a special case when a job is cancelled before getting logged in the scheduler
            task_logger.warning(
                "job data cannot be retrieved from qstat."
                " likely the job is cancelled before recording."
                " setting job status to CANCELLED"
            )
            submit_time, start_time, end_time = [0] * 3
            n_cores = 0.0
            run_time = 0
            status = "CANCELLED"
            return start_time, end_time, run_time, n_cores, status

        tasks_dict = json_dict["Jobs"]
        assert (
            len(tasks_dict.keys()) == 1
        ), f"Too many tasks returned by qstat: {tasks_dict.keys()}"

        task_name = list(tasks_dict.keys())[0]
        task_dict = tasks_dict[task_name]
        submit_time = task_dict["ctime"].replace(" ", "_")
        start_time = task_dict["qtime"].replace(" ", "_")
        # Last modified time. There isn't an explicit end time,
        # so only other option would be to add walltime to start time
        end_time = task_dict["mtime"].replace(" ", "_")
        # check if 'resources_used' are one of the fields
        if "resources_used" in task_dict.keys():
            n_cores = float(task_dict["resources_used"]["ncpus"])
            run_time = task_dict["resources_used"]["walltime"]
        else:
            # give a dummy data when pbs failed to return json with required field
            n_cores = 1
            run_time = "00:00:01"

        # status uses the same states as the queue monitor, rather than full words like sacct
        status = task_dict["job_state"]

        return start_time, end_time, run_time, n_cores, status

    HEADER_TEMPLATE = "pbs_header.cfg"
    STATUS_DICT = {"R": 3, "Q": 2, "E": 3, "F": 4}
    SCRIPT_EXTENSION = "pbs"
    QUEUE_NAME = "qstat"

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
        out, err = self._run_command_and_wait([f"qsub {script_location}"], shell=True)
        os.chdir(cwd)
        self.logger.debug((out, err))

        if len(err) != 0:
            raise self.raise_exception(
                f"An error occurred during job submission: {err} \n {script_location}"
            )

        self.logger.debug("Successfully submitted task to pbs")
        # no errors, return the job id
        return_words = out.split(".pbs")  # 4027812.pbs
        self.logger.debug(return_words)

        try:
            jobid = int(return_words[0])
        except ValueError:
            raise self.raise_exception(
                f"{return_words[0]} is not a valid jobid. Submitting the job most likely failed. The return message was {out}"
            )

        out, err = self._run_command_and_wait([f"qstat {jobid}"], shell=True)
        try:
            job_name = out.split("\n")[2].split()[1]
        except Exception:
            raise self.raise_exception(
                "Unable to determine job name from qstat. Exiting"
            )

        self.logger.debug(f"Return from qstat, stdout: {out}, stderr:{err}")

        f_name = f"{job_name}_{timestamp}_{jobid}"
        # Set the error and output logs to <name>_<time>_<job_id> as this cannot be done before submission time
        self.logger.debug(
            f"Setting output files for task {jobid} to {sim_dir}/{f_name}.out/.err"
        )
        self._run_command_and_wait(
            [f"qalter -o {sim_dir}/{f_name}.out {jobid}"], shell=True
        )
        self._run_command_and_wait(
            [f"qalter -e {sim_dir}/{f_name}.err {jobid}"], shell=True
        )
        return jobid

    def cancel_job(self, job_id: int, target_machine=None) -> None:
        return self._run_command_and_wait(cmd=[f"qdel {job_id}"], shell=True)

    def check_queues(self, user: bool = False, target_machine=None) -> List[str]:
        self.logger.debug(
            f"Checking queues with raw input of machine {target_machine} and user {user}"
        )
        if user:  # just print the list of jobid and status (a space between)
            cmd = [f"qstat -u {self.user_name}"]
            header_pattern = "pbs:"
            header_idx = 1
            job_list_idx = 5
        else:
            cmd = ["qstat"]
            header_pattern = "Job id"
            header_idx = 0
            job_list_idx = 3

        (output, err) = self._run_command_and_wait(cmd, encoding="utf-8", shell=True)
        self.logger.debug(f"Command {cmd} got response output {output} and error {err}")
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

    def check_wct(self, job_id: int):
        """
        Checks the given job_id if it has hit Wall Clock Time
        :param job_id: The id of the job to be checked for wct
        :return: Boolean for if the job has hit Wall Clock Time or not
        """
        cmd = f"qstat -x {job_id} -f -F json | grep walltime"
        output, err = self._run_command_and_wait(cmd=[cmd], shell=True)

        print("PERFOMED QSTAT WALLTIME")
        print(output)
        print(type(output))

        return True

        # job_info = eval(output)["Jobs"][f"{job_id}.pbs"]
        # limit_hour, limit_min, limit_sec = job_info["Resource_List"]["walltime"].split(":") # 2nd one
        # limit_time = timedelta(hours=int(limit_hour), minutes=int(limit_min), seconds=int(limit_sec))
        # elapsed_hour, elapsed_min, elapsed_sec = job_info["resources_used"]["walltime"].split(":")
        # elapsed_time = timedelta(hours=int(elapsed_hour), minutes=int(elapsed_min), seconds=int(elapsed_sec))
        # return elapsed_time > limit_time

    @staticmethod
    def process_arguments(
        script_path: str,
        arguments: Dict[str, str],
        scheduler_arguments: Dict[str, str] = [],
    ):
        """
        keys in arguments must match whatever the pbs script is expecting, otherwise will fail
        """
        # maps scheduler specific args with commands
        scheduler_header_command_dict = {
            "time": "-l walltime={value}",
            "job_name": "-N {value}",
            "ncpus": "-l ncpus={value}",
            "nodes": "-l select={value}",
        }
        scheduler_args_commands = ""
        for key, value in scheduler_arguments.items():
            if key in scheduler_header_command_dict.keys():
                scheduler_args_commands = (
                    scheduler_args_commands
                    + " "
                    + scheduler_header_command_dict[key].format(value=value)
                )
        # script related args
        # construct a string
        args_string = ""
        for arg in arguments.items():
            # using "" to make sure variables are tranlated and not taken literally
            args_string = args_string + f'{arg[0]}="{arg[1]}",'
        return f"{scheduler_args_commands} -v {args_string} -V {script_path} "
