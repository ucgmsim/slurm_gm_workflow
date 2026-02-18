#!/usr/bin/env python3
"""
Script to complete a broken v25p10 deployment.

This handles only the realization-specific operations (step 5/5) that failed
in the original deployment due to the e3d.par path bug.
"""

import argparse
from pathlib import Path
import shutil
import os
from natsort import natsorted


def create_modified_config_file(
    original_file_path,
    modified_file_path,
    old_base_paths,
    new_base_path,
    fixed_value_overrides=None,
):
    """
    Read original config file, modify paths, and write to modified_file_path.

    First replaces all occurrences of each path in old_base_paths with new_base_path,
    then applies any fixed value overrides for key=value or key: value lines.

    Parameters
    ----------
    original_file_path : Path or str
        Path to the original config file
    modified_file_path : Path or str
        Path to write the modified config file
    old_base_paths : list[str] or str
        Old base directory path(s) to replace. Can be a single string or a list of strings.
    new_base_path : Path or str
        New base directory to replace the old path(s) with
    fixed_value_overrides : dict, optional
        Dictionary of {key: value} pairs to override specific keys in the config file.
        Supports both '=' delimiter (.par files) and ':' delimiter (YAML files).
    """
    # Normalise to a list so callers can pass a single string or a list
    if isinstance(old_base_paths, str):
        old_base_paths = [old_base_paths]

    with open(original_file_path, "r") as f:
        content = f.read()

    # Replace all occurrences of each old base path with the new one
    modified_content = content
    for old_base_path in old_base_paths:
        modified_content = modified_content.replace(old_base_path, str(new_base_path))

    # If there are fixed value overrides, process line by line to apply them
    if fixed_value_overrides:
        modified_lines = []
        for line in modified_content.splitlines(keepends=True):
            stripped = line.strip()

            # Skip empty lines and comments
            if not stripped or stripped.startswith("#"):
                modified_lines.append(line)
                continue

            # Determine the delimiter and extract the key
            delimiter = None
            key = None

            if "=" in stripped:
                delimiter = "="
                key = stripped.split("=", 1)[0].strip()
            elif ":" in stripped:
                delimiter = ": "
                key = stripped.split(":", 1)[0].strip()

            # Check if this key needs a fixed value override
            if key and key in fixed_value_overrides:
                # Preserve leading whitespace (indentation) for YAML files
                leading_whitespace = line[: len(line) - len(line.lstrip())]
                modified_lines.append(
                    f"{leading_whitespace}{key}{delimiter}{fixed_value_overrides[key]}\n"
                )
                continue

            modified_lines.append(line)

        modified_content = "".join(modified_lines)

    # Ensure the output directory exists
    modified_file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(modified_file_path, "w") as f:
        f.write(modified_content)


def main():
    parser = argparse.ArgumentParser(
        description="Complete broken v25p10 deployment (realization processing only)"
    )
    parser.add_argument(
        "--fault",
        default="WaimeaS",
        help="Fault name (default: WaimeaS)",
    )
    args = parser.parse_args()

    version = "v25p10"
    fault = args.fault

    # Fixed value overrides for e3d.par files
    E3D_PAR_FIXED_VALUES = {
        "wcc_prog_dir": '"/scratch/projects/rch-quakecore/EMOD3D_old_Cybershake/tools/emod3d-mpi_v3.0.8"',
        "vel_mod_params_dir": f'"/scratch/projects/rch-quakecore/Cybershake/{version}/Data/VMs/{fault}"',
        "grid_file": '""',
        "model_params": '""',
    }

    base_cybershake_dir = Path("/scratch/projects/rch-quakecore/Cybershake")

    old_base_paths_to_replace = [
        "/uoc/project/uoc40001/scratch/baes/Cybershake",
        "/gpfs/scratch/cant1/Cybershake",
        "/nesi/nobackup/nesi00213/RunFolder/Cybershake",
        "/scratch/hpc11a02/gmsim/RunFolder/Cybershake",
    ]

    realizations_dir = (
        base_cybershake_dir
        / "setup_files_from_dropbox"
        / version
        / "permanent_small_files"
        / "extracted"
        / f"{version}_configs_params"
        / fault
    )

    if not realizations_dir.exists():
        print(f"ERROR: Realizations directory not found: {realizations_dir}")
        return 1

    realizations = natsorted(
        [d for d in os.listdir(realizations_dir) if os.path.isdir(realizations_dir / d)]
    )

    print(f"\n{'='*60}")
    print(f"Completing broken deployment for version={version}, fault={fault}")
    print(f"Total realizations to process: {len(realizations)}")
    print(f"{'='*60}\n")

    print("[5/5] Processing realizations...")

    total_realizations = len(realizations)
    for idx, realization in enumerate(realizations, 1):
        print(f"  [{idx}/{total_realizations}] Processing {realization}...")

        # v25p10 paths
        lf_output_source_path = (
            base_cybershake_dir
            / "setup_files_from_dropbox"
            / version
            / "large_temp_files"
            / "extracted"
            / version
            / "LF"
            / fault
            / f"{realization}_LF_OutBin"
            / realization
            / "LF"
        )

        lf_output_destination_path = (
            base_cybershake_dir / version / "Runs" / fault / realization / "LF"
        )

        # For v25p10, e3d.par is inside the LF directory
        original_e3d_par_file_path = lf_output_source_path / "e3d.par"

        modified_e3d_par_file_path = (
            base_cybershake_dir
            / version
            / "Runs"
            / fault
            / realization
            / "LF"
            / "e3d.par"
        )

        # Create modified sim_params.yaml file
        original_sim_params_file_path = (
            base_cybershake_dir
            / "setup_files_from_dropbox"
            / version
            / "permanent_small_files"
            / "extracted"
            / f"{version}_configs_params"
            / fault
            / realization
            / "sim_params.yaml"
        )

        modified_sim_params_file_path = (
            base_cybershake_dir
            / version
            / "Runs"
            / fault
            / realization
            / "sim_params.yaml"
        )

        # Ensure parent directory exists before moving
        lf_output_destination_path.parent.mkdir(parents=True, exist_ok=True)

        # Move LF directory if not already moved
        if not lf_output_destination_path.exists():
            if lf_output_source_path.exists():
                shutil.move(lf_output_source_path, lf_output_destination_path)
                print(f"    Moved LF directory")
            else:
                print(f"    ERROR: Source LF directory not found: {lf_output_source_path}")
                continue
        else:
            print(f"    LF directory already exists at destination")

        # CRITICAL FIX: Update e3d.par path to the new location after move
        original_e3d_par_file_path = lf_output_destination_path / "e3d.par"

        # Create modified e3d.par file
        if not modified_e3d_par_file_path.exists():
            if original_e3d_par_file_path.exists():
                create_modified_config_file(
                    original_file_path=original_e3d_par_file_path,
                    modified_file_path=modified_e3d_par_file_path,
                    old_base_paths=old_base_paths_to_replace,
                    new_base_path=base_cybershake_dir,
                    fixed_value_overrides=E3D_PAR_FIXED_VALUES,
                )
                print(f"    Created e3d.par")
            else:
                print(f"    ERROR: e3d.par not found: {original_e3d_par_file_path}")
        else:
            print(f"    e3d.par already exists, skipping")

        # Create modified sim_params.yaml file
        if not modified_sim_params_file_path.exists():
            if original_sim_params_file_path.exists():
                create_modified_config_file(
                    original_file_path=original_sim_params_file_path,
                    modified_file_path=modified_sim_params_file_path,
                    old_base_paths=old_base_paths_to_replace,
                    new_base_path=base_cybershake_dir,
                )
                print(f"    Created sim_params.yaml")
            else:
                print(f"    ERROR: sim_params.yaml source not found: {original_sim_params_file_path}")
        else:
            print(f"    sim_params.yaml already exists, skipping")

    print(f"\n{'='*60}")
    print("Completion script finished!")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    exit(main())
