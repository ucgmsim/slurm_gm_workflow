"""Contains class and helper functions for end to end test"""
import sys
import os
import json
import shutil
import time
import glob
import subprocess
from collections import namedtuple
from typing import List
from threading import Thread
from queue import Queue, Empty

import pandas as pd
import sqlite3 as sql
from pandas.util.testing import assert_frame_equal

from scripts.management.db_helper import connect_db_ctx
from scripts.management.MgmtDB import SlurmTask
from shared_workflow.shared import exe
import qcore.constants as const
import qcore.simulation_structure as sim_struct


def get_sim_dirs(runs_dir):
    """Gets all simualation dirs under the specified Runs dir.
    Also returns the fault dirs. Full paths.
    """
    sim_dirs = []
    fault_dirs = get_faults(runs_dir)
    for fault in fault_dirs:
        fault_name = os.path.basename(fault)

        entries = os.listdir(fault)
        for entry in entries:
            entry_path = os.path.join(fault, entry)
            if entry.startswith(fault_name) and os.path.isdir(entry_path):
                sim_dirs.append(entry_path)

    return fault_dirs, sim_dirs


def get_faults(runs_dir: str):
    """Gets all the fault directories in the specified Runs dir.
    Full path.
    """
    return [
        os.path.join(runs_dir, entry)
        for entry in os.listdir(runs_dir)
        if os.path.isdir(os.path.join(runs_dir, entry))
    ]


Error = namedtuple("Error", ["location", "error"])
Warning = namedtuple("Warning", ["location", "warning"])


class E2ETests(object):
    """Class responsible for setting up, running and checking end-to-end tests
    based on the input config file
    """

    # Config keys
    cf_test_dir_key = "test_dir"
    cf_data_dir_key = "data_dir"
    cf_cybershake_config_key = "cybershake_config"
    cf_fault_list_key = "fault_list"
    cf_bench_folder_key = "bench_dir"
    test_checkpoint_key = "test_checkpoint"
    timeout_key = "timeout"

    # Benchmark folders
    bench_IM_csv_folder = "IM_csv"

    # Log files
    install_out_file = "install_out_log.txt"
    install_err_file = "install_err_log.txt"

    submit_out_file = "submit_out_log.txt"
    submit_err_file = "submit_err_log.txt"

    queue_out_file = "queue_out_log.txt"
    queue_err_file = "queue_err_log.txt"

    warnings_file = "warnings_log.txt"
    errors_file = "errors_log.txt"

    # Error Keywords
    error_keywords = ["error", "traceback", "exception"]

    # Templates to check for
    expected_templates = [
        "run_bb_mpi.sl.template",
        "run_emod3d.sl.template",
        "run_hf_mpi.sl.template",
        "sim_im_calc.sl.template",
        "post_emod3d_merge_ts.sl.template",
        "post_emod3d_winbin_aio.sl.template",
    ]

    def __init__(self, config_file: str):
        """Constructor, reads input config."""

        with open(config_file, "r") as f:
            self.config_dict = json.load(f)

        # Add tmp directory
        self.stage_dir = os.path.join(
            self.config_dict[self.cf_test_dir_key], "tmp_{}".format(const.timestamp)
        )

        self.im_bench_folder = os.path.join(
            self.config_dict[self.cf_bench_folder_key], self.bench_IM_csv_folder
        )
        self.timeout = self.config_dict[self.timeout_key] * 60

        self.warnings, self.errors = [], []
        self.fault_dirs, self.sim_dirs = [], []
        self.runs_dir = None

        self._sim_passed, self._sim_failed = set(), set()
        self._stop_on_error = None

        # Resources that need to be dealt with on close
        self._processes = []
        self._files = []

    def run(
        self,
        user: str,
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

        self.check_install()
        if self.errors and self._stop_on_error:
            print("Quitting due to the following errors:")
            self.print_errors()
            return False

        # Run automated workflow
        if not self._run_auto(user, sleep_time=sleep_time):
            return False
        # Only check that everything is completed, when auto submit does not
        # exit early
        else:
            self.check_mgmt_db()

        if self.errors:
            print("The following errors occurred during the automated workflow:")
            self.print_errors()
        else:
            print("It appears there were no errors during the automated workflow!")
            if not no_clean_up:
                self.teardown()

        return True

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

    def setup(self):
        """Setup for automatic workflow

        Change this to use the qcore simulation structure functions!!
        """
        print("Running setup...")
        print("Using directory {}".format(self.stage_dir))

        # Create tmp dir
        os.mkdir(self.stage_dir)

        # Data
        data_dir = os.path.join(self.stage_dir, "Data")
        shutil.copytree(self.config_dict[self.cf_data_dir_key], data_dir)

        # Cybershake config
        shutil.copy(self.config_dict[self.cf_cybershake_config_key], self.stage_dir)

        # Fault list
        shutil.copy(self.config_dict[self.cf_fault_list_key], self.stage_dir)

        # Create runs folder
        os.mkdir(os.path.join(self.stage_dir, "Runs"))

        # Mgmt queue
        os.mkdir(os.path.join(self.stage_dir, "mgmt_db_queue"))

        self.runs_dir = sim_struct.get_runs_dir(self.stage_dir)

    def install(self):
        """Install the automated workflow

        Runs install bash script, saves output into log files in the
        staging directory. Also checks for error keywords in the output
        and saves warnings accordingly.
        """

        # Why is this a script? Make it all python?
        script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../scripts/cybershake/install_cybershake.sh",
        )
        cmd = "{} {} {} {}".format(
            script_path,
            self.stage_dir,
            os.path.join(
                self.stage_dir,
                os.path.basename(self.config_dict[self.cf_cybershake_config_key]),
            ),
            os.path.join(
                self.stage_dir,
                os.path.basename(self.config_dict[self.cf_fault_list_key]),
            ),
        )

        print("Running install...")
        out_file = os.path.join(self.stage_dir, self.install_out_file)
        err_file = os.path.join(self.stage_dir, self.install_err_file)
        with open(out_file, "w") as out_f, open(err_file, "w") as err_f:
            exe(cmd, debug=False, stdout=out_f, stderr=err_f)

        # Check for errors
        # Get these straight from execution?
        output = open(out_file, "r").read()
        error = open(err_file, "r").read()
        if any(cur_str in output.lower() for cur_str in self.error_keywords):
            msg = "There appears to be errors in the install. Error keyword found in stdout!"
            print(msg)
            print("##### INSTALL OUTPUT #####")
            print(output)
            print("##########################")
            self.warnings.append(Warning("Install - Stdout", msg))

        if any(cur_str in error.lower() for cur_str in self.error_keywords):
            msg = "There appears to be errors in the install. Error keyword found in stderr!"
            print(msg)
            print("##### INSTALL OUTPUT #####")
            print(error)
            print("##########################")
            self.warnings.append(Warning("Install - Stderr", msg))

        self.fault_dirs, self.sim_dirs = get_sim_dirs(self.runs_dir)

    def _check_true(self, check: bool, location: str, error_msg: str):
        if not check:
            self.errors.append(Error(location, error_msg))

    def check_install(self):
        """Checks that all required templates exists, along with the yaml params """

        for sim_dir in self.sim_dirs:
            # Check sim_params.yaml are there
            self._check_true(
                "sim_params.yaml" in os.listdir(sim_dir),
                "Install - Sim params",
                "Sim params file is missing in {}".format(sim_dir),
            )

        # Check fault params
        for fault in self.fault_dirs:
            self._check_true(
                "fault_params.yaml" in os.listdir(fault),
                "Install - Fault params",
                "Fault params are missing in {}".format(fault),
            )

        # Check root params
        self._check_true(
            "root_params.yaml" in os.listdir(self.runs_dir),
            "Install - root params",
            "Root params are missing in {}".format(self.runs_dir),
        )

    def _run_auto(self, user: str, sleep_time: int = 10):
        """
        Runs auto submit

        Parameters
        ----------
        user: str
            The username under which to run the tasks
        sleep_time: int
            Time (in seconds) between progress checks
        """
        submit_cmd = (
            "python3 {} {} {} --sleep_time 2 --config {} "
            "--no_merge_ts --no_clean_up".format(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "../scripts/cybershake/auto_submit.py",
                ),
                self.stage_dir,
                user,
                sim_struct.get_cybershake_config(self.stage_dir),
            )
        )
        queue_cmd = "python3 {} {}".format(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "../scripts/cybershake/queue_monitor.py",
            ),
            self.stage_dir,
        )

        # Different process types for which canceling/resume is tested
        proc_type_cancel = None
        if self.config_dict[self.test_checkpoint_key]:
            proc_type_cancel = [
                const.ProcessType.EMOD3D,
                const.ProcessType.HF,
                const.ProcessType.BB,
            ]

        # Have to put this in a massive try block, to ensure that
        # the run_queue_and_auto_submit process is terminated on any errors.
        try:
            print("Starting auto submit...")
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

            print("Starting mgmt queue monitor...")
            p_queue = exe(
                queue_cmd,
                debug=False,
                non_blocking=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self._processes.append(p_queue)
            p_queue_out_nbsr = NonBlockingStreamReader(p_queue.stdout)
            p_queue_err_nbsr = NonBlockingStreamReader(p_queue.stderr)

            # Create and open the log files
            out_submit_f = open(os.path.join(self.stage_dir, self.submit_out_file), "w")
            err_submit_f = open(os.path.join(self.stage_dir, self.submit_err_file), "w")
            out_queue_f = open(os.path.join(self.stage_dir, self.queue_out_file), "w")
            err_queue_f = open(os.path.join(self.stage_dir, self.queue_err_file), "w")
            self._files.extend((out_submit_f, err_submit_f, out_queue_f, err_queue_f))

            # Monitor mgmt db
            print("Progress: ")
            start_time = time.time()
            while time.time() - start_time < self.timeout:
                try:
                    total_count, comp_count, failed_count = (
                        self.check_mgmt_db_progress()
                    )
                    if not self.check_completed():
                        return False
                except sql.OperationalError as ex:
                    print(
                        "Operational error while accessing database. "
                        "Retrying in {} seconds\n{}".format(sleep_time, ex)
                    )
                    continue

                print(
                    "Completed: {}, Failed: {}, Total: {}".format(
                        comp_count, failed_count, total_count
                    )
                )

                # Get the log data
                for file, reader in [
                    (out_submit_f, p_submit_out_nbsr),
                    (err_submit_f, p_submit_err_nbsr),
                    (out_queue_f, p_queue_out_nbsr),
                    (err_queue_f, p_queue_err_nbsr),
                ]:
                    lines = reader.readlines()
                    if lines:
                        file.writelines(lines)
                        file.flush()

                if proc_type_cancel:
                    proc_type_cancel = self.cancel_running(proc_type_cancel)

                if total_count == (comp_count + failed_count):
                    break
                else:
                    time.sleep(sleep_time)

            if time.time() - start_time > self.timeout:
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

    def cancel_running(self, proc_types: List[const.ProcessType]):
        """Looks for any running task of the specified process types
        and attempts to cancel one of each.
        """
        # Get all running jobs in the mgmt db
        proc_types_int = [proc_type.value for proc_type in proc_types]
        with connect_db_ctx(sim_struct.get_mgmt_db(self.stage_dir)) as cur:
            entries = cur.execute(
                "SELECT proc_type, job_id, run_name  FROM state "
                "WHERE status == 3 AND proc_type IN ({}?)".format(
                    "?," * (len(proc_types_int) - 1)
                ),
                (*proc_types_int,),
            ).fetchall()

        # Cancel one for each process type
        for entry in entries:
            if entry[0] in proc_types_int:
                print(
                    "Checkpoint testing: Cancelling job-id {} "
                    "for {} and process type {}".format(entry[1], entry[2], entry[0])
                )
                out, err = exe("scancel -v {}".format(entry[1]), debug=False)

                print("Scancel out: ", out, err)
                if "error" not in out.lower() and "error" not in err.lower():
                    proc_types.remove(const.ProcessType(entry[0]))
                    proc_types_int.remove(entry[0])
                    print("Cancelled job-id {}".format(entry[1]))

        return proc_types

    def check_mgmt_db(self):
        """Create errors for all entries in management db that did not complete"""
        with connect_db_ctx(sim_struct.get_mgmt_db(self.stage_dir)) as cur:
            entries = cur.execute(
                "SELECT run_name, proc_type, status, job_id, retries FROM state "
                "WHERE proc_type <=6 AND status != 4"
            ).fetchall()

        entries = [SlurmTask(*entry) for entry in entries]

        for entry in entries:
            if entry.status != const.Status.completed.value:
                self.errors.append(
                    Error(
                        "Slrum task",
                        "Run {} did not complete task {} "
                        "(Status {}, Retries {}, JobId {}".format(
                            entry.run_name,
                            const.ProcessType(entry.proc_type),
                            const.Status(entry.status),
                            entry.retries,
                            entry.job_id,
                        ),
                    )
                )

    def check_sim_result(self, sim_dir: str):
        """Checks that all the LF, HF and BB binaries are there and that the
        IM values match up with the benchmark IMs
        """
        result = True

        # Check HF binary
        hf_bin = sim_struct.get_hf_bin_path(sim_dir)
        if not os.path.isfile(hf_bin):
            self.errors.append(
                Error("HF - Binary", "The HF binary is not at {}".format(hf_bin))
            )
            result = False

        # Check BB binary
        bb_bin = sim_struct.get_bb_bin_path(sim_dir)
        if not os.path.isfile(bb_bin):
            self.errors.append(
                Error("BB - Binary", "The BB binary is not at {}".format(hf_bin))
            )
            result = False

        # Check IM
        im_csv = sim_struct.get_IM_csv(sim_dir)
        if not os.path.isfile(im_csv):
            self.errors.append(
                Error(
                    "IM_calc - CSV", "The IM_calc csv file is not at {}".format(im_csv)
                )
            )
            result = False
        else:
            bench_csv = os.path.join(
                self.im_bench_folder,
                "{}.csv".format(os.path.basename(sim_dir).split(".")[0]),
            )
            bench_df = pd.read_csv(bench_csv)
            cur_df = pd.read_csv(im_csv)

            try:
                assert_frame_equal(cur_df, bench_df)
            except AssertionError:
                self.errors.append(
                    Error(
                        "IM - Values",
                        "The IMs for {} are not equal to the benchmark {}".format(
                            im_csv, bench_csv
                        ),
                    )
                )
                result = False

        return result

    def check_mgmt_db_progress(self):
        """Checks auto submit progress in the management db"""
        with connect_db_ctx(sim_struct.get_mgmt_db(self.stage_dir)) as cur:
            total_count = cur.execute(
                "SELECT COUNT(*) FROM state WHERE proc_type <= 6"
            ).fetchone()[0]
            comp_count = cur.execute(
                "SELECT COUNT(*) FROM state WHERE status == 4 AND proc_type <= 6"
            ).fetchone()[0]
            failed_count = cur.execute(
                "SELECT COUNT(*) FROM state WHERE status == 5 AND proc_type <= 6"
            ).fetchone()[0]

        return total_count, comp_count, failed_count

    def check_completed(self):
        """Checks all simulations that have completed"""
        with connect_db_ctx(sim_struct.get_mgmt_db(self.stage_dir)) as cur:
            completed_sims = cur.execute(
                "SELECT run_name FROM state WHERE proc_type == 6 AND status = 4"
            ).fetchall()
        completed_sims = [sim_t[0] for sim_t in completed_sims]

        # Only check the ones that haven't been checked already
        completed_new = set(completed_sims) - (self._sim_passed | self._sim_failed)

        for sim in completed_new:
            result = self.check_sim_result(
                os.path.join(
                    self.runs_dir, sim_struct.get_fault_from_realisation(sim), sim
                )
            )

            if not result:
                self._sim_failed.add(sim)

                if self._stop_on_error:
                    print("Quitting as the following errors occured: ")
                    self.print_errors()
                    return False
                else:
                    print("The following error occured for simulation {}:".format(sim))
                    print(
                        "ERROR: {}, {}\n".format(
                            self.errors[-1].location, self.errors[-1].error
                        )
                    )

            else:
                self._sim_passed.add(sim)

        print(
            "Passed/Failed/Total simulations: {}/{}/{}, ".format(
                len(self._sim_passed), len(self._sim_failed), len(self.sim_dirs)
            )
        )

        return True

    def teardown(self):
        """Remove all files created during the end-to-end test"""
        print("Deleting everything under {}".format(self.stage_dir))
        shutil.rmtree(self.stage_dir)


class NonBlockingStreamReader:
    """A non-blocking stream reader.

    Based on http://eyalarubas.com/python-subproc-nonblock.html
    """

    def __init__(self, stream):
        """
        stream: the stream to read from.
                Usually a process' stdout or stderr.
        """

        self._s = stream
        self._q = Queue()

        def _populate_queue(stream, queue):
            """
            Collect lines from 'stream' and put them in 'queue'.
            """

            while True:
                line = stream.readline()
                if line:
                    queue.put(line)
                else:
                    print("Stream has been closed.")
                    sys.exit()

        self._t = Thread(target=_populate_queue, args=(self._s, self._q))
        self._t.daemon = True
        self._t.start()  # start collecting lines from the stream

    def readlines(self):
        """Reads the lines from the queue, returns None if the queue is empty"""
        lines = []
        cur_line = ""
        while cur_line is not None:
            try:
                cur_line = self._q.get(block=False)
            except Empty:
                cur_line = None

            if cur_line is not None:
                lines.append(cur_line)

        if lines:
            return lines
        return None
