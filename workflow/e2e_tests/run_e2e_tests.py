#!/usr/bin/env python3
"""Script to run end-to-end tests"""
import argparse
import signal

from workflow.e2e_tests.E2ETests import E2ETests
from workflow.e2e_tests.queue_monitor_tests import QueueMonitorStressTest
from workflow.automation.lib.schedulers.scheduler_factory import Scheduler


def on_exit(signum, frame):
    test_object.close()
    exit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "config_file", type=str, help="Config file for the end-to-end test"
    )
    parser.add_argument(
        "user", type=str, help="The username under which to run the tasks"
    )
    parser.add_argument(
        "--sleep_time",
        type=int,
        default=5,
        help="Sleep time (in seconds) between mgmt db progress checks.",
    )
    parser.add_argument(
        "--stop_on_warning", action="store_true", help="Stop execution on warnings"
    )
    parser.add_argument(
        "--stop_on_error", action="store_true", help="Stop execution on errors"
    )
    parser.add_argument(
        "--no_clean_up",
        action="store_true",
        help="Prevent deletion of the test directory, even when there are no errors.",
    )
    parser.add_argument(
        "--test_restart",
        action="store_true",
        help="Stop and start the workflow wrapper multiple times to test resume functionality.",
    )
    parser.add_argument(
        "--test_queue",
        action="store_true",
        help="Run the stress test for the queue_monitor.",
    )

    args = parser.parse_args()

    signal.signal(signal.SIGINT, on_exit)

    test_object = None

    Scheduler.initialise_scheduler(args.user)

    if args.test_queue:
        test_object = QueueMonitorStressTest(args.config_file)
        test_object.run(
            sleep_time=args.sleep_time,
            stop_on_error=args.stop_on_error,
            stop_on_warning=args.stop_on_warning,
            no_clean_up=args.no_clean_up,
        )
    else:
        test_object = E2ETests(args.config_file)
        test_object.run(
            args.user,
            sleep_time=args.sleep_time,
            stop_on_error=args.stop_on_error,
            stop_on_warning=args.stop_on_warning,
            no_clean_up=args.no_clean_up,
            test_restart=args.test_restart,
        )
