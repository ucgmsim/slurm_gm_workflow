import pytest

from workflow.automation.lib.schedulers.scheduler_factory import Scheduler


@pytest.fixture(scope="session")
def init_scheduler():
    print("Initialising scheduler")
    Scheduler.initialise_scheduler("test_user")
