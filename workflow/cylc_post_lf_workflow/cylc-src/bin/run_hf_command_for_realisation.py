#!/usr/bin/env python

import argparse
import os
import subprocess
import sys
from qcore import utils

BINARY_PATH="/scratch/projects/rch-quakecore/EMOD3D_old_Cybershake/tools"
def main():
    parser = argparse.ArgumentParser(description="Generate HF simulation command from sim_params.yaml")
    parser.add_argument("rel_dir", help="Path to REL_DIR containing sim_params.yaml")
    args = parser.parse_args()

    # Construct path to sim_params.yaml
    sim_params_path = os.path.join(args.rel_dir, "sim_params.yaml")

    try:
        print(f"Loading parameters from: {sim_params_path}")
        params = utils.load_sim_params(sim_yaml_path=sim_params_path)
        print("Parameters loaded successfully")

        hf_params = params["hf"]

        gmsim = "/home/arr65/src"
        
        # Get number of MPI tasks from SLURM environment
        ntasks = os.environ.get("SLURM_NTASKS")
        if ntasks is None:
            raise RuntimeError("SLURM_NTASKS environment variable not set. Are you running under SLURM?")
        print(f"Using {ntasks} MPI tasks")
        
        command = f"srun --quit-on-interrupt --kill-on-bad-exit=1 -n {ntasks} python {gmsim}/slurm_gm_workflow/workflow/calculation/hf_sim.py " \
                  f"{params['FD_STATLIST']} " \
                  f"{args.rel_dir}/HF/Acc/HF.bin " \
                  f"--hf_vel_mod_1d {hf_params['hf_vel_mod_1d']} " \
                  f"--duration {params['sim_duration']} " \
                  f"--dt {hf_params['dt']} " \
                  f"--sim_bin {BINARY_PATH}/hb_high_binmod_v{hf_params['version']} " \
                  f"--version {hf_params['version']} " \
                  f"--rvfac {hf_params['rvfac']} " \
                  f"--sdrop {hf_params['sdrop']} " \
                  f"--path_dur {hf_params['path_dur']} " \
                  f"--kappa {hf_params['kappa']} " \
                  f"--seed {hf_params['seed']} " \
                  f"--slip {hf_params['slip']}"

        # Create output directories if they don't exist
        output_dir = os.path.join(args.rel_dir, "HF", "Acc")
        os.makedirs(output_dir, exist_ok=True)

        print(f"Running command: {command}")
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(f"Subprocess completed with return code: {result.returncode}")
        if result.returncode != 0:
            print(f"Command failed with return code {result.returncode}", file=sys.stderr)
            print(f"stderr: {result.stderr}", file=sys.stderr)
            sys.exit(1)
        print("Command completed successfully")

    except Exception as e:
        print(f"Error generating command: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

