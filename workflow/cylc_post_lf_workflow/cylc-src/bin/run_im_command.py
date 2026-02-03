#!/usr/bin/env python

import argparse
import os
import subprocess
import sys
from qcore import utils

def main():
    parser = argparse.ArgumentParser(description="Generate and run IM calculation command from sim_params.yaml")
    parser.add_argument("rel_dir", help="Path to REL_DIR containing sim_params.yaml")
    args = parser.parse_args()

    # Construct path to sim_params.yaml
    sim_params_path = os.path.join(args.rel_dir, "sim_params.yaml")

    try:
        print(f"Loading parameters from: {sim_params_path}")
        params = utils.load_sim_params(sim_yaml_path=sim_params_path)
        print("Parameters loaded successfully")

        ims = params["ims"]
        rel_name = os.path.basename(args.rel_dir)  # e.g., "MS09_REL01"
        fault_dir = os.path.dirname(args.rel_dir)
        fault_name = os.path.basename(fault_dir)   # e.g., "MS09"

        # Construct paths
        bb_bin = os.path.realpath(os.path.join(args.rel_dir, "BB", "Acc", "BB.bin"))
        im_out_dir = os.path.realpath(os.path.join(args.rel_dir, "IM"))

        # Verify BB.bin exists
        if not os.path.isfile(bb_bin):
            print(f"ERROR: BB.bin not found at {bb_bin}", file=sys.stderr)
            sys.exit(1)
        print(f"BB.bin found: {bb_bin}")

        # Create output directory if it doesn't exist
        os.makedirs(im_out_dir, exist_ok=True)
        print(f"Output directory: {im_out_dir}")

        # Get version from params if available
        version = params.get("version")

        gmsim = "/home/arr65/src"
        
        # Get number of MPI tasks from SLURM environment
        # IM calculation requires at least 2 tasks (1 server + N workers)
        ntasks = os.environ.get("SLURM_NTASKS")
        if ntasks is None:
            raise RuntimeError("SLURM_NTASKS environment variable not set. Are you running under SLURM?")
        if int(ntasks) < 2:
            raise RuntimeError(f"IM calculation requires at least 2 MPI tasks (1 server + N workers), got {ntasks}")
        print(f"Using {ntasks} MPI tasks")
        
        command = [
            "srun", "-n", ntasks, "python",
            f"{gmsim}/IM_calculation/IM_calculation/scripts/calculate_ims_mpi.py",
            bb_bin,
            "b",
            "-o", im_out_dir,
            "-i", rel_name,      # identifier: MS09_REL01
            "-r", fault_name,    # rupture: MS09
            "-t", "s",           # run_type: simulated
            "-v", version,       # version
        ]

        # Add component list
        if "component" in ims:
            command.append("-c")
            for comp in ims["component"]:
                command.append(str(comp))

        # Add -e if extended_period is True
        if ims.get("extended_period", False):
            command.append("-e")

        # Always add -s for simple output
        command.append("-s")

        # Add pSA periods
        if "pSA_periods" in ims:
            command.append("-p")
            for period in ims["pSA_periods"]:
                command.append(str(period))

        # Print the command for debugging
        command_str = " ".join(command)
        print(f"Running command: {command_str}")
        print("-" * 60)

        # Run the command, letting stdout/stderr flow through directly
        result = subprocess.run(command, capture_output=True, text=True)
        
        # Always print stdout if there is any
        if result.stdout:
            print("=== STDOUT ===")
            print(result.stdout)
        
        # Always print stderr if there is any
        if result.stderr:
            print("=== STDERR ===")
            print(result.stderr)

        print("-" * 60)
        print(f"Subprocess completed with return code: {result.returncode}")
        
        if result.returncode != 0:
            print(f"ERROR: Command failed with return code {result.returncode}", file=sys.stderr)
            sys.exit(1)
        
        print("IM calculation completed successfully")

    except Exception as e:
        import traceback
        print(f"Error in run_im_command.py: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
