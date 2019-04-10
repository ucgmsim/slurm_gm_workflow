#!/usr/bin/env python3
"""Script to run end-to-end tests"""
import argparse

from e2e_tests.E2ETests import E2ETests

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
        default=10,
        help="Sleep time (in seconds) between mgmt db progress checks.",
    )
    parser.add_argument(
        "--stop_on_warning",
        action="store_true",
        default=None,
        help="Stop execution on warnings",
    )
    parser.add_argument(
        "--stop_on_error",
        action="store_true",
        default=None,
        help="Stop execution on errors",
    )
    parser.add_argument(
        "--no_clean_up",
        action="store_true",
        default=None,
        help="Prevent deletion of the test directory, even when there are no errors.",
    )

    args = parser.parse_args()

    e2e_test = E2ETests(args.config_file)
    e2e_test.run(
        args.user,
        sleep_time=args.sleep_time,
        stop_on_error=args.stop_on_error,
        stop_on_warning=args.stop_on_warning,
        no_clean_up=args.no_clean_up,
    )
