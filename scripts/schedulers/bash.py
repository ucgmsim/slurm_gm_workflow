from scripts.schedulers.scheduler import Scheduler


class Bash(Scheduler):
    job_counter = 0
    task_running = False

    RUN_COMMAND = ""
    # TODO: Update to empty header?
    HEADER_TEMPLATE = "slurm_header.cfg"

    def submit_job(self, script_location: str, target_machine: str = None):
        """
        Runs job in the bash shell
        Will run job until completion
        Enforces that the script is executable, then executes it
        :param script_location:
        :param target_machine:
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
        raise self.raise_scheduler_exception(
            "Cannot cancel a job with the bash scheduler"
        )

    def check_queues(self, user: str = None, target_machine=None):
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
            tasks.append((f"{self.job_counter}", "R"))
        return tasks
