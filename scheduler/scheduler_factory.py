from logging import Logger

from qcore.constants import PLATFORM_CONFIG
from qcore.config import host
from qcore.qclogging import get_basic_logger
from scheduler.abstractscheduler import AbstractScheduler
from scheduler.bash import Bash
from scheduler.pbs import Pbs

from scheduler.slurm import Slurm
from scripts.common.platform_config import platform_config


class Scheduler:
    __scheduler = None

    @classmethod
    def initialise_scheduler(
        cls, user: str, account: str = None, logger: Logger = get_basic_logger()
    ):
        if cls.__scheduler is not None:
            raise RuntimeError("Scheduler already initialised")

        scheduler = platform_config[PLATFORM_CONFIG.SCHEDULER.name]
        if account is None:
            account = platform_config[PLATFORM_CONFIG.DEFAULT_ACCOUNT.name]
        platform_accounts = [account]
        if PLATFORM_CONFIG.PLATFORM_ACCOUNTS.name in platform_config:
            platform_accounts = platform_config[PLATFORM_CONFIG.PLATFORM_ACCOUNTS.name]
        if scheduler == "slurm":
            cls.__scheduler = Slurm(
                user=user,
                account=account,
                current_machine=host,
                logger=logger,
                platform_accounts=platform_accounts,
            )
        elif scheduler == "pbs":
            cls.__scheduler = Pbs(
                user=user, account=account, current_machine=host, logger=logger
            )
        else:
            cls.__scheduler = Bash(
                user=user, account=account, current_machine=host, logger=logger
            )
        cls.__scheduler.logger.debug("Scheduler initialised")

    @classmethod
    def get_scheduler(cls) -> AbstractScheduler:
        """Returns the scheduler appropriate for the current machine/platform environment. Should be called"""
        if cls.__scheduler is None:
            raise RuntimeError(
                "Scheduler has not been initialised. Run initialise_scheduler first."
            )
        return cls.__scheduler
