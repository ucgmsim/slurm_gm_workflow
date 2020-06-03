from abc import ABC, abstractmethod
from logging import Logger
from typing import List

from qcore.shared import exe
from qcore.qclogging import VERYVERBOSE, NOPRINTERROR


class SchedulerException(EnvironmentError):
    pass


class Scheduler(ABC):
    """
    Defines the generic scheduler API to interact with various platform scheduling software
    """

    RUNCOMMAND: str

    def __init__(self, user, account, logger: Logger):
        self.user_name = user
        self.account = account
        self.logger = logger

    @abstractmethod
    def submit_job(self, script_location: str, target_machine: str = None) -> int:
        """
        Submits jobs to the platforms scheduler. Returns the job id of the submitted job.
        :param script_location: The absolute path to the script to be submitted
        :param target_machine: The machine the job is to be submitted to
        :return: The job id of the submitted job
        """
        pass

    @abstractmethod
    def cancel_job(self, job_id: int, target_machine=None) -> None:
        """
        Cancels the job with the given job id
        :param job_id: The id of the job to be cancelled
        :param target_machine: The machine the job is running on
        """
        pass

    @abstractmethod
    def check_queues(
        self, user: str = None, target_machine=None
    ) -> List[str]:
        """
        Checks the schedulers queue(s) for running jobs
        :param user: Which user should the jobs be checked for?
        :param target_machine: The machine to check the queues of
        :return: A list of jobs and states, in the format "<job id> <state>"
        """
        pass

    def logging_wrapper(self, func):
        """
        Wraps an external function so that all input and output is logged at the VERYVERBOSE level
        :param func: The function to be wrapped
        :return: The wrapped function
        """

        def wrapper(*args, **kwargs):
            res = func(*args, **kwargs)
            self.logger.log(
                VERYVERBOSE,
                f"Command {func.__name__} run with args {args} and kwargs {kwargs}. Result: {res}",
            )
            return res

        return wrapper

    __run_command_and_wait = logging_wrapper(exe)

    # def __run_command_and_wait(self, cmd, debug=True, shell=False, stdout=True, stderr=True, stdin=None):
    #     """
    #     Runs a command and logs the output to the logger under a very verbose level.
    #     Effectively a wrapper for qcore.shared.exe with logging
    #     :param cmd: The command to be run
    #     :param kwargs: Any other named arguments to be passed to exe
    #     :return:
    #     """
    #     out, err = exe(cmd, debug=debug, shell=shell, stdout=True, stderr=True, stdin=None)
    #     self.logger.log(VERYVERBOSE, f"{cmd} out: {out}, err: {err}")
    #     return out, err

    def raise_scheduler_exception(self, message) -> SchedulerException:
        """
        Logs and raises an exception related to scheduler interaction
        :param message: The message of the exception
        :return: The exception
        """
        self.logger.log(NOPRINTERROR, message)
        return SchedulerException(message)
