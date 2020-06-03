from logging import Logger

from qcore.config import qconfig, host
from qcore.qclogging import get_basic_logger
from scripts.schedulers.bash import Bash

from scripts.schedulers.scheduler import Scheduler
from scripts.schedulers.slurm import Slurm

__scheduler = None


def initialise_scheduler(
    scheduler, user: str, account: str, logger: Logger = get_basic_logger()
):
    global __scheduler
    if __scheduler is not None:
        raise RuntimeError("Scheduler already initialised")

    # if qconfig["scheduler"] == "slurm":
    if scheduler == "slurm":
        __scheduler = Slurm(
            user=user, account=account, current_machine=host, logger=logger
        )
    # elif qconfig["scheduler"] == "pbs":
    elif scheduler == "pbs":
        pass
    else:
        __scheduler = Bash(user=user, account=account, logger=logger)
    __scheduler.logger.debug("Scheduler initialised")


def get_scheduler() -> Scheduler:
    """Returns the scheduler appropriate for the current machine/platform environment. Should be called"""
    global __scheduler
    if __scheduler is None:
        raise RuntimeError(
            "Scheduler has not been initialised. Run initialise_scheduler first."
        )
    return __scheduler
