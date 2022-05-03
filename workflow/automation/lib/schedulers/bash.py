from logging import Logger
from typing import Dict

from workflow.automation.lib.MgmtDB import SchedulerTask
from workflow.automation.lib.schedulers.abstractscheduler import AbstractScheduler


class Bash(AbstractScheduler):
    job_counter = 0
    task_running = False

    SCRIPT_EXTENSION = "sh"
    QUEUE_NAME = "ps"
    STATUS_DICT = {"R": 3}

    @staticmethod
    def process_arguments(script_path: str, arguments: Dict[str, str]):
        return f"{script_path} {' '.join(arguments.items())}"

    def get_metadata(self, db_running_task: SchedulerTask, task_logger: Logger):
        status = "RUNNING" if self.task_running else "FAILED"
        return 0, 0, 0, 1, status

    def submit_job(self, sim_dir: str, script_location: str, target_machine: str = None, **kwargs):
        """
        Runs job in the bash shell
        Will run job until completion
        Enforces that the script is executable, then executes it
        :param script_location:
        :param target_machine:
        :param **kwargs: Any additional parameters to be passed to the executor
        :return: The index of the task
        """
        self.logger.debug(
            f"Ensuring execute set and running script located at: {script_location}"
        )

        # Could run task as non blocking with internal "schedule manager".
        # May result in cpu overload if too many tasks are run at once
        self.task_running = True
        self._run_command_and_wait(cmd=f"chmod u+x {script_location}")
        self._run_command_and_wait(cmd=f"{script_location}")
        self.task_running = False
        self.job_counter += 1
        return self.job_counter

    def cancel_job(self, job_id: int, target_machine=None):
        raise self.raise_exception("Cannot cancel a job with the bash scheduler")

    def check_queues(self, user: bool = False, target_machine=None):
        """
        If there is a job running in submit_job then this returns the job number
        submit_job needs to be changed to non-blocking to allow the task to enter the 'queued' state in the db
        :param user: The user to check the queues for (unused)
        :param target_machine: The machine to check the queues of (unused)
        :return: Either an empty list or a list containing a job id, 'R' tuple to represent the currently running job
        """
        self.logger.debug("Bash scheduler queues are empty")
        tasks = []
        if self.task_running:
            tasks.append((f"{self.job_counter} R"))
        return tasks

    def check_wct(self, job_id: int):
        pass
