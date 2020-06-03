from scripts.schedulers.scheduler import Scheduler


class Bash(Scheduler):
    job_counter = 0

    def submit_job(self, script_location):
        """Current implementation of the submit """
        self.logger.debug(
            f"Ensuring execute set and running script located at: {script_location}"
        )
        self.__run_command_and_wait(cmd=f"chmod u+x {script_location}")
        self.__run_command_and_wait(cmd=f"{script_location}")
        self.job_counter += 1
        return self.job_counter

    def cancel_job(self, job_id):
        raise self.raise_scheduler_exception(
            "Cannot cancel a job with the bash scheduler"
        )

    def check_queues(self, only_user_jobs=False):
        self.logger.debug("Bash scheduler queues are empty")
        return []
