#!/usr/bin/env python

import argparse
import os
import subprocess
import sys
from qcore import utils


def main():
    parser = argparse.ArgumentParser(
        description="Generate LF2BB conversion command from sim_params.yaml"
    )
    parser.add_argument("rel_dir", help="Path to REL_DIR containing sim_params.yaml")
    args = parser.parse_args()

    sim_params_path = os.path.join(args.rel_dir, "sim_params.yaml")

    try:
        print(f"Loading parameters from: {sim_params_path}")
        params = utils.load_sim_params(sim_yaml_path=sim_params_path)
        print("Parameters loaded successfully")

        bb_params = params.get("bb", {})

        lf_outbin = os.path.join(args.rel_dir, "LF", "OutBin")
        bb_output = os.path.join(args.rel_dir, "BB", "Acc", "BB.bin")

        vs30_file = params.get("stat_vs_est")
        if vs30_file is None:
            raise KeyError("stat_vs_est not found in sim_params.yaml")

        # dt is optional for lf2bb (defaults to LF's native dt when omitted)
        if "dt" in bb_params:
            dt = bb_params["dt"]
        elif "dt" in params:
            dt = params["dt"]
        else:
            dt = None

        gmsim = "/home/arr65/src"

        cmd_parts = [
            "python",
            f"{gmsim}/slurm_gm_workflow/workflow/calculation/lf2bb.py",
            lf_outbin,
            vs30_file,
            bb_output,
        ]
        if dt is not None:
            cmd_parts += ["--dt", str(dt)]
        command = " ".join(cmd_parts)

        output_dir = os.path.join(args.rel_dir, "BB", "Acc")
        os.makedirs(output_dir, exist_ok=True)

        print(f"Running command: {command}")
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        print(f"Subprocess completed with return code: {result.returncode}")
        if result.stdout:
            print(result.stdout)
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
