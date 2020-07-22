from abc import ABC, abstractmethod
from collections import OrderedDict
from logging import Logger
from typing import List, Dict, Type, Optional

from qcore.shared import exe
from qcore.qclogging import VERYVERBOSE, NOPRINTERROR
from scripts.management.MgmtDB import SchedulerTask


def task_runner_no_debug(*args, **kwargs):
    return exe(*args, debug=False, **kwargs)


class SchedulerException(EnvironmentError):
    pass


class AbstractScheduler(ABC):
    """
    Defines the generic scheduler API to interact with various platform scheduling software
    """

    STATUS_DICT: Dict[str, int]
    SCRIPT_EXTENSION: str
    HEADER_TEMPLATE: str
    QUEUE_NAME: str

    def __init__(self, user, account, current_machine, logger: Logger):
        self.user_name = user
        self.account = account
        self.current_machine = current_machine
        self.logger = logger
        self._run_command_and_wait = self.logging_wrapper(task_runner_no_debug)

    @abstractmethod
    def submit_job(
        self, sim_dir, script_location: str, target_machine: str = None
    ) -> int:
        """
        Submits jobs to the platforms scheduler. Returns the job id of the submitted job.
        Automatically sets the output/error file names
        :param sim_dir: The realisation directory
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
        self, user: Optional[str] = None, target_machine=None
    ) -> List[str]:
        """
        Checks the schedulers queue(s) for running jobs
        :param user: Which user should the jobs be checked for?
        :param target_machine: The machine to check the queues of
        :return: A list of jobs and states, in the format "<job id> <state>"
        """
        pass

    @staticmethod
    @abstractmethod
    def process_arguments(script_path: str, arguments: OrderedDict[str, str]):
        """
        Processes the script path and arguments to return them in a format usable by the scheduler
        :param script_path: The path to the script (or command to be run)
        :param arguments: Any arguments to be passed to the script
        :return: The string to be executed
        """
        pass

    @abstractmethod
    def get_metadata(self, db_running_task: SchedulerTask, task_logger: Logger):
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

    def raise_exception(
        self, message, exception_type: Type[Exception] = SchedulerException
    ) -> Exception:
        """
        Logs and raises an exception related to scheduler interaction
        :param exception_type:
        :param message: The message of the exception
        :return: The exception
        """
        self.logger.log(NOPRINTERROR, message)
        return exception_type(message)
