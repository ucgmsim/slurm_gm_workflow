"""For stress testing queue_monitor"""
import os
import json
import shutil
import time
import subprocess

import numpy.random as nprdm
import sqlite3 as sql

import qcore.constants as const
import qcore.simulation_structure as sim_struct
from qcore.shared import exe
from workflow.automation.execution_scripts.add_to_mgmt_queue import add_to_queue
from workflow.automation.install_scripts import create_mgmt_db
from workflow.automation.lib.MgmtDB import connect_db_ctx
from workflow.e2e_tests.E2ETests import NonBlockingStreamReader, Error


class QueueMonitorStressTest(object):

    # Config keys
    cf_test_dir_key = "test_dir"
    test_checkpoint_key = "test_checkpoint"
    timeout_key = "timeout"
    realisation_count_key = "realisations"
    tasks_key = "tasks_to_complete"
    task_count_key = "simultaneous_task_count"

    # Doesn't really matter
    realisation_name = "Hossack"

    submit_out_file = "submit_out_log.txt"
    submit_err_file = "submit_err_log.txt"

    possible_task_states = 3

    def __init__(self, config_file: str):

        with open(config_file, "r") as f:
            self.config_dict = json.load(f)

        self.stage_dir = os.path.join(
            self.config_dict[self.cf_test_dir_key], "tmp_{}".format(const.timestamp)
        )
        self.mgmt_dir = os.path.join(self.stage_dir, "mgmt_db_queue")

        self.warnings, self.errors = [], []

        self._stop_on_error = None
        self._processes = []
        self._files = []

        self.timeout = self.config_dict[self.timeout_key] * 60
        self.realisations = self.config_dict[self.realisation_count_key]
        self.tasks = self.config_dict[self.tasks_key]
        self.task_state = [0] * self.realisations
        self.realisation_task_states = len(self.tasks) * self.possible_task_states

        self.tasks_to_submit_at_once = self.config_dict[self.task_count_key]

        self.jid = 0

    def setup(self):
        print("Running setup...")
        print("Using directory {}".format(self.stage_dir))

        # Create tmp dir
        os.mkdir(self.stage_dir)

        # Mgmt queue
        os.mkdir(self.mgmt_dir)

    def install(self):
        print("Installing database")
        create_mgmt_db.create_mgmt_db(
            [
                "{}_REL{:0>2}".format(self.realisation_name, i)
                for i in range(1, 1 + self.realisations)
            ],
            sim_struct.get_mgmt_db(self.stage_dir),
        )

    def run(
        self,
        sleep_time: int = 10,
        stop_on_error: bool = True,
        stop_on_warning: bool = False,
        no_clean_up: bool = False,
    ):
        """
        Runs the full automated workflow and checks that everything works as
        expected. Prints out a list of errors, if there are any.

        The test directory is deleted if there are no errors, unless no_clean_up
        is set.

        Parameters
        ----------
        user: str
            The username under which to run the tasks
        """
        self._stop_on_error = stop_on_error

        # Setup folder structure
        self.setup()

        # Run install script
        self.install()
        if self.warnings and stop_on_warning:
            print("Quitting due to warnings following warnings:")
            self.print_warnings()
            return False

        # Run automated workflow
        if not self._run_auto(sleep_time=sleep_time):
            return False
        # Only check that everything is completed, when auto submit does not
        # exit early

        if self.errors:
            print("The following errors occurred during the automated workflow:")
            self.print_errors()
        else:
            print("It appears there were no errors during the automated workflow!")
            if not no_clean_up:
                self.teardown()

        return True

    def _generate_rel_list(self):
        remaining_tasks = self.realisations - self.task_state.count(
            self.realisation_task_states
        )
        if remaining_tasks <= self.tasks_to_submit_at_once:
            return [
                i
                for i, x in enumerate(self.task_state)
                if x != self.realisation_task_states
            ]
        rels = []
        while len(rels) < self.tasks_to_submit_at_once:
            num = nprdm.randint(self.realisations)
            if self.task_state[num] != self.realisation_task_states and num not in rels:
                rels.append(num)
        counts = [0, 0, 0]
        for i in rels:
            counts[self.task_state[i] % self.possible_task_states] += 1
        print(
            "Tasks from created to queued: {}. Tasks from queued to running: {}. Tasks from running to completed: {}.".format(
                *counts
            )
        )
        return rels

    def _run_auto(self, sleep_time: int = 10):
        """
        Runs auto submit

        Parameters
        ----------
        user: str
            The username under which to run the tasks
        sleep_time: int
            Time (in seconds) between progress checks
        """
        submit_cmd = "python {} {} --sleep_time 2".format(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "../automation/execution_scripts/queue_monitor.py",
            ),
            self.stage_dir,
        )

        # Have to put this in a massive try block, to ensure that
        # the run_queue_and_auto_submit process is terminated on any errors.
        try:
            print("Starting cybershake wrapper...")
            p_submit = exe(
                submit_cmd,
                debug=False,
                non_blocking=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self._processes.append(p_submit)
            p_submit_out_nbsr = NonBlockingStreamReader(p_submit.stdout)
            p_submit_err_nbsr = NonBlockingStreamReader(p_submit.stderr)

            # Create and open the log files
            out_submit_f = open(os.path.join(self.stage_dir, self.submit_out_file), "w")
            err_submit_f = open(os.path.join(self.stage_dir, self.submit_err_file), "w")
            self._files.extend((out_submit_f, err_submit_f))
            outputs_to_check = [
                (out_submit_f, p_submit_out_nbsr),
                (err_submit_f, p_submit_err_nbsr),
            ]

            # Monitor mgmt db
            print("Progress: ")
            start_time = time.time()
            while time.time() - start_time < self.timeout:
                try:
                    comp_count, total_count = self.check_mgmt_db_progress()
                except sql.OperationalError as ex:
                    print(
                        "Operational error while accessing database. "
                        "Retrying in {} seconds\n{}".format(sleep_time, ex)
                    )
                    time.sleep(sleep_time)
                    continue
                print(
                    "Created/Queued/Running/Completed/Total/Expected simulations: {}/{}/{}/{}/{}/{}".format(
                        *comp_count, total_count, len(self.tasks) * self.realisations
                    )
                )

                # Get the log data
                for file, reader in outputs_to_check:
                    lines = reader.readlines()
                    if lines:
                        file.writelines(lines)
                        file.flush()

                for rel in self._generate_rel_list():
                    self._add_task_to_queue(rel, self.task_state[rel])
                    self.task_state[rel] += 1

                if total_count == comp_count[-1]:
                    break
                else:
                    time.sleep(sleep_time)

            if time.time() - start_time >= self.timeout:
                print("The auto-submit timeout expired.")
                self.errors.append(
                    Error("Auto-submit timeout", "The auto-submit timeout expired.")
                )
                return False
        # Still display the exception
        except Exception as ex:
            raise ex
        # Clean up
        finally:
            self.close()

        return True

    def close(self):
        """Terminates any running processes and closes any open files"""
        for p in self._processes:
            if p is not None:
                p.terminate()
        for f in self._files:
            if f is not None:
                f.close()

    def check_mgmt_db_progress(self):
        """Checks auto submit progress in the management db"""
        with connect_db_ctx(sim_struct.get_mgmt_db(self.stage_dir)) as cur:
            comp_count = [
                cur.execute(
                    "SELECT COUNT(*) "
                    "FROM state "
                    "WHERE status = ? "
                    "AND proc_type in (?{})".format(",?" * (len(self.tasks) - 1)),
                    (i, *self.tasks),
                ).fetchone()[0]
                for i in range(1, 5)
            ]
            total_count = cur.execute(
                "SELECT COUNT(*) FROM state "
                "WHERE proc_type in (?{})".format(",?" * (len(self.tasks) - 1)),
                (*self.tasks,),
            ).fetchone()[0]

        return comp_count, total_count

    def _add_task_to_queue(self, rel_num: int, proc_task_state: int):
        proc_type = self.tasks[proc_task_state // self.possible_task_states]
        status = 2 + proc_task_state % self.possible_task_states

        job_id = self.jid
        self.jid += 1

        add_to_queue(
            self.mgmt_dir,
            "{}_REL{:0>2}".format(self.realisation_name, rel_num + 1),
            proc_type,
            status,
            job_id,
        )

    def teardown(self):
        """Remove all files created during the end-to-end test"""
        print("Deleting everything under {}".format(self.stage_dir))
        shutil.rmtree(self.stage_dir)

    def print_warnings(self):
        with open(os.path.join(self.stage_dir, self.warnings_file), "a") as f:
            for warn in self.warnings:
                text = "WARNING: {}, {}".format(warn.location, warn.warning)
                print(text)
                f.write(text)

    def print_errors(self):
        with open(os.path.join(self.stage_dir, self.errors_file), "a") as f:
            for err in self.errors:
                text = "ERROR: {}, {}\n".format(err.location, err.error)
                print(text)
                f.write(text)
