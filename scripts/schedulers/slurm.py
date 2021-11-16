from logging import Logger
from typing import Union, Dict, Optional
from os.path import join

from qcore.constants import ProcessType, timestamp

from scripts.management.MgmtDB import SchedulerTask
from scripts.schedulers.abstractscheduler import AbstractScheduler
from shared_workflow.platform_config import HPC, get_target_machine


class Slurm(AbstractScheduler):
    STATUS_DICT = {"R": 3, "PD": 2, "CG": 3}
    SCRIPT_EXTENSION = "sl"
    QUEUE_NAME = "squeue"
    HEADER_TEMPLATE = "slurm_header.cfg"

    def check_queues(self, user: bool = False, target_machine: HPC = None):
        self.logger.debug(
            f"Checking queues for machine {target_machine}{f' and user {self.user_name}' if user else ''}."
        )

        if target_machine is None:
            target_machine = self.current_machine
        if isinstance(self.account, dict):
            account = self.account[target_machine.name]
        else:
            account = self.account
        accounts = ",".join(self.platform_accounts)
        if user:
            # user is True, so we use the same user as we use for submission
            cmd = f"squeue -A {accounts} -o '%A %t' -M {target_machine.name} -u {self.user_name}"
        else:
            cmd = f"squeue -A {accounts} -o '%A %t' -M {target_machine.name}"
        self.logger.debug(f"Running squeue command: {cmd}")
        output, err = self._run_command_and_wait(cmd=[cmd], shell=True)
        self.logger.debug(f"Squeue got output: {output}")
        self.logger.debug(f"Squeue got err: {err}")
        message = ""

        try:
            header = output.split("\n")[1]
        except Exception:
            message = f"squeue did not return expected output. Ignoring for this iteration. Actual output: {output}"
        else:
            if header != "JOBID ST":
                message = f"squeue did not return expected output. Ignoring for this iteration. Actual output: {output}"

        if message:
            raise EnvironmentError(message)

        output_list = list(filter(None, output.split("\n")[1:]))
        output_list.pop(0)
        return output_list

    def submit_job(self, sim_dir, script_location, target_machine=None):
        """Submits the slurm script and updates the management db
        :param sim_dir:
        """
        self.logger.debug(
            "Submitting {} on machine {}".format(script_location, target_machine)
        )
        f_name = f"%x_{timestamp}_%j"
        if isinstance(self.account, dict):
            account = self.account[target_machine]
        else:
            account = self.account
        common_pre = f"sbatch -o {join(sim_dir, f'{f_name}.out')} -e {join(sim_dir, f'{f_name}.err')} -A {account}"
        if target_machine and target_machine != self.current_machine:
            mid = f"--export=CUR_ENV,CUR_HPC -M {target_machine}"
        else:
            mid = ""
        command = " ".join([common_pre, mid, script_location])
        self.logger.debug(f"Submitting command {command}")
        out, err = self._run_command_and_wait(cmd=[command], shell=True)

        if len(err) == 0 and out.startswith("Submitted"):
            self.logger.debug("Successfully submitted task to slurm")
            # no errors, return the job id
            return_words = out.split()
            job_index = return_words.index("job")
            jobid = return_words[job_index + 1]
            try:
                int(jobid)
            except ValueError as e:
                self.logger.critical(
                    f"{jobid} is not a valid jobid. "
                    f"Submitting the job most likely failed. "
                    f"The return message was {out}"
                )
                raise e
            return jobid
        else:
            raise self.raise_exception(
                f"An error occurred during job submission: {err}"
            )

    def cancel_job(self, job_id, target_machine=None):
        if target_machine is None:
            machine_to_target = self.current_machine
        else:
            machine_to_target = target_machine

        self.logger.debug(f"Cancelling {job_id} on machine {machine_to_target}")

        if target_machine and machine_to_target != self.current_machine:
            command = f"scancel -M {target_machine} {job_id}"
        else:
            command = f"scancel {job_id}"

        out, err = self._run_command_and_wait(cmd=[command], shell=True)

        if "error" not in out.lower() and "error" not in err.lower():
            self.logger.debug(f"Cancelled job-id {job_id} successfully")
        else:
            raise self.raise_exception(
                f"An error occurred during job cancellation: {err}"
            )
        return out, err

    @staticmethod
    def process_arguments(
        script_path: str,
        arguments: Dict[str, str],
        scheduler_arguments: Dict[str, str],
    ):
        # maps scheduler specific args with commands
        scheduler_header_command_dict = {
            "time": "--time={value}",
            "job_name": "--job-name={value}",
            "ncpus": "--cpus-per-task={value}",
            "nodes": "--nodes={value}",
            "ntasks": "--ntasks={value}",
        }
        scheduler_args_commands = ""
        for key, value in scheduler_arguments.items():
            if key in scheduler_header_command_dict.keys():
                scheduler_args_commands = (
                    scheduler_args_commands
                    + " "
                    + scheduler_header_command_dict[key].format(value=value)
                )

        return f"{scheduler_args_commands} {script_path} {' '.join(arguments.values())}"

    def get_metadata(self, db_running_task: SchedulerTask, task_logger: Logger):
        """
        TODO: Check Tacc compatibility
        :param db_running_task: The task to retrieve metadata for
        :param task_logger: the logger for the task
        :return:
        """
        target_machine = get_target_machine(ProcessType(db_running_task.proc_type))
        cmd = f"sacct -n -X -j {db_running_task.job_id} -M {target_machine} -o 'jobid%10,jobname%35,Submit,Start,End,NCPUS,CPUTimeRAW%18,State,Nodelist%60'"
        out, err = self._run_command_and_wait(cmd, shell=True)
        output = out.strip().split()
        # ['578928', 'u-bl689.atmos_main.18621001T0000Z', '2019-08-16T13:05:06', '2019-08-16T13:12:56', '2019-08-16T14:58:28', '1840', '11650880', 'CANCELLED+', 'nid00[166-171,180-196]']
        try:
            submit_time, start_time, end_time = [
                x.replace("T", "_") for x in output[2:5]
            ]
            n_cores = float(output[5])
            run_time = float(output[6]) / n_cores
            status = output[7]

        except Exception:
            # a special case when a job is cancelled before getting logged in sacct
            task_logger.warning(
                "job data cannot be retrieved from sacct. likely the job is cancelled before recording. setting job status to CANCELLED"
            )
            submit_time, start_time, end_time = [0] * 3
            n_cores = 0.0
            run_time = 0
            status = "CANCELLED"

        return start_time, end_time, run_time, n_cores, status
