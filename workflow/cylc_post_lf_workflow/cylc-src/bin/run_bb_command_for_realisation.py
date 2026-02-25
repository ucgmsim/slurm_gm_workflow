#!/usr/bin/env python

import argparse
import os
import subprocess
import sys
from qcore import utils

def main():
    parser = argparse.ArgumentParser(description="Generate BB simulation command from sim_params.yaml")
    parser.add_argument("rel_dir", help="Path to REL_DIR containing sim_params.yaml")
    args = parser.parse_args()

    # Construct path to sim_params.yaml
    sim_params_path = os.path.join(args.rel_dir, "sim_params.yaml")

    try:
        print(f"Loading parameters from: {sim_params_path}")
        params = utils.load_sim_params(sim_yaml_path=sim_params_path)
        print("Parameters loaded successfully")

        # Get BB parameters from sim_params
        # Note: flo is at root level, not under bb section
        bb_params = params.get("bb", {})
        
        # Calculate paths similar to run_bb_rch.sl
        # REL_DIR = args.rel_dir
        # FAULT_DIR = dirname(REL_DIR)
        # RUNS_DIR = dirname(FAULT_DIR)  # Gets to "Runs" directory
        # BASE_DIR = dirname(RUNS_DIR)   # Gets to v25p11 (or equivalent base)
        # JOBNAME = basename(REL_DIR)
        fault_dir = os.path.dirname(args.rel_dir)
        runs_dir = os.path.dirname(fault_dir)  # /path/to/v25p11/Runs
        base_dir = os.path.dirname(runs_dir)   # /path/to/v25p11
        jobname = os.path.basename(args.rel_dir)
        fault_name = os.path.basename(fault_dir)

        # Construct paths
        lf_outbin = os.path.join(args.rel_dir, "LF", "OutBin")
        vm_dir = os.path.join(base_dir, "Data", "VMs", fault_name)
        hf_bin = os.path.join(args.rel_dir, "HF", "Acc", "HF.bin")
        bb_output = os.path.join(args.rel_dir, "BB", "Acc", "BB.bin")
        
        # Get VS30 file path from sim_params or use default
        vs30_file = params.get("stat_vs_est")
        # Get BB parameters from sim_params
        # flo is at root level (not under bb section)
        flo = params["flo"]
        # fmin and fmidbot are under bb section
        fmin = bb_params["fmin"]
        fmidbot = bb_params["fmidbot"]
        # dt can be in bb section or at root level (prefer bb if present)
        if "dt" in bb_params:
            dt = bb_params["dt"]
        elif "dt" in params:
            dt = params["dt"]
        else:
            raise KeyError("dt not found in bb section or root level of sim_params.yaml")

        gmsim = "/home/arr65/src"
        
        # Get number of MPI tasks from SLURM environment
        ntasks = os.environ.get("SLURM_NTASKS")
        if ntasks is None:
            raise RuntimeError("SLURM_NTASKS environment variable not set. Are you running under SLURM?")
        print(f"Using {ntasks} MPI tasks")
        
        command = f"srun -n {ntasks} python {gmsim}/slurm_gm_workflow/workflow/calculation/bb_sim.py " \
                  f"{lf_outbin} " \
                  f"{vm_dir} " \
                  f"{hf_bin} " \
                  f"{vs30_file} " \
                  f"{bb_output} " \
                  f"--flo {flo} " \
                  f"--fmin {fmin} " \
                  f"--fmidbot {fmidbot} " \
                  f"--dt {dt}"

        # Create output directories if they don't exist
        output_dir = os.path.join(args.rel_dir, "BB", "Acc")
        os.makedirs(output_dir, exist_ok=True)

        log_file = os.path.join(output_dir, "bb_run.log")
        print(f"Running command: {command}")
        print(f"Output will be logged to: {log_file}")
        with open(log_file, "w") as log:
            result = subprocess.run(command, shell=True, stdout=log, stderr=subprocess.STDOUT)
        print(f"Subprocess completed with return code: {result.returncode}")
        if result.returncode != 0:
            print(f"Command failed with return code {result.returncode}", file=sys.stderr)
            print(f"See log for details: {log_file}", file=sys.stderr)
            sys.exit(1)
        print("Command completed successfully")

    except Exception as e:
        print(f"Error generating command: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
