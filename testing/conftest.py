import pytest

from scripts.schedulers.scheduler_factory import initialise_scheduler


@pytest.fixture(scope="session")
def init_scheduler():
    print("Initialising scheduler")
    initialise_scheduler("test_user")