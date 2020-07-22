import pytest

from scripts.schedulers.scheduler_factory import Scheduler


@pytest.fixture(scope="session")
def init_scheduler():
    print("Initialising scheduler")
    Scheduler.initialise_scheduler("test_user")