#!/usr/bin/env python3
"""
Script to deploy files for post-LF stages of the Cybershake workflow.

Copies and modifies configuration files, moves source files, VMs, and LF output
data from the Dropbox download staging area to their final locations.
"""

import argparse
from pathlib import Path
import shutil
import os
import sys
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


def check_path(path, description, errors):
    """Check if a path exists and report the result. Appends to errors list on failure."""
    if not path.exists():
        print(f"    MISSING {description}: {path}")
        errors.append(f"{description}: {path}")


def main():
    parser = argparse.ArgumentParser(
        description="Deploy files for post-LF stages of the Cybershake workflow"
    )
    parser.add_argument("version", help="Version string (e.g., v25p11)")
    parser.add_argument("fault", help="Fault name (e.g., NMFZB1)")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only check that source files exist, without deploying anything",
    )
    args = parser.parse_args()

    version = args.version
    fault = args.fault
    check_only = args.check
    check_errors = []

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

    ll_and_vs30_files_provided_in_this_version = "v25p11"  # for versions before this, the .ll and .vs30 files are not provided in the Dropbox download and need to be copied from an older version

    realizations_dir = (
        base_cybershake_dir
        / "setup_files_from_dropbox"
        / version
        / "permanent_small_files"
        / "extracted"
        / f"{version}_configs_params"
        / fault
    )
    realizations = natsorted(
        [d for d in os.listdir(realizations_dir) if os.path.isdir(realizations_dir / d)]
    )

    # =============================================================================
    # Operations that depend only on version (not fault or realization)
    # commented out as these do not need to be changed between runs of the same version
    # =============================================================================

    # # root_params.yaml is the same (and in the same place) for all faults and realisations
    # # Create modified root_params.yaml file
    original_root_params_file_path = (
        base_cybershake_dir
        / "setup_files_from_dropbox"
        / version
        / "permanent_small_files"
        / "extracted"
        / f"{version}_configs_params"
        / "root_params.yaml"
    )

    modified_root_params_file_path = (
        base_cybershake_dir / version / "Runs" / "root_params.yaml"
    )

    if check_only:
        check_path(
            original_root_params_file_path, "root_params.yaml source", check_errors
        )
    else:
        create_modified_config_file(
            original_file_path=original_root_params_file_path,
            modified_file_path=modified_root_params_file_path,
            old_base_paths=old_base_paths_to_replace,
            new_base_path=base_cybershake_dir,
            fixed_value_overrides={
                "hf_vel_mod_1d": "/scratch/projects/rch-quakecore/Cybershake/VelocityModel/Mod-1D/Cant1D_v3-midQ_OneRay.1d",
                "mgmt_db_location": "",
            },
        )

    # ## These files are the same (and in the same place) for all faults
    # # # copy .ll and .vs30 files
    original_ll_and_vs30_source_path = (
        base_cybershake_dir
        / "setup_files_from_dropbox"
        / ll_and_vs30_files_provided_in_this_version
        / "permanent_small_files"
        / "extracted"
        / "VMs"
        / f"{ll_and_vs30_files_provided_in_this_version}_setup_files"
    )

    destination_ll_and_vs30_path = base_cybershake_dir / version

    ll_file = (
        original_ll_and_vs30_source_path
        / "non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.ll"
    )
    vs30_file = (
        original_ll_and_vs30_source_path
        / "non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.vs30"
    )

    if check_only:
        check_path(ll_file, ".ll source", check_errors)
        check_path(vs30_file, ".vs30 source", check_errors)
    else:
        destination_ll_and_vs30_path.mkdir(parents=True, exist_ok=True)
        shutil.copy(ll_file, destination_ll_and_vs30_path)
        shutil.copy(vs30_file, destination_ll_and_vs30_path)

    # =============================================================================
    # Operations that depend on fault (but not realization)
    # =============================================================================
    mode_label = "Checking" if check_only else "Deploying"
    print(f"\n{'='*60}")
    print(f"{mode_label} files for version={version}, fault={fault}")
    print(f"Total realizations to process: {len(realizations)}")
    print(f"{'='*60}\n")

    print("[1/5] Creating modified fault_params.yaml file...")
    original_fault_params_file_path = (
        base_cybershake_dir
        / "setup_files_from_dropbox"
        / version
        / "permanent_small_files"
        / "extracted"
        / f"{version}_configs_params"
        / fault
        / "fault_params.yaml"
    )

    destination_fault_params_base_base = base_cybershake_dir / version / "Runs" / fault
    modified_fault_params_file_path = (
        destination_fault_params_base_base / "fault_params.yaml"
    )

    if check_only:
        check_path(
            original_fault_params_file_path, "fault_params.yaml source", check_errors
        )
    else:
        destination_fault_params_base_base.mkdir(parents=True, exist_ok=True)
        create_modified_config_file(
            original_file_path=original_fault_params_file_path,
            modified_file_path=modified_fault_params_file_path,
            old_base_paths=old_base_paths_to_replace,
            new_base_path=base_cybershake_dir,
        )
        print(f"    Created: {modified_fault_params_file_path}")

    print("[2/5] Copying .ll and .statcords files...")
    # copy ll and statscords
    if version == "v25p11":
        original_ll_statcords_source_path = (
            base_cybershake_dir
            / "setup_files_from_dropbox"
            / version
            / "permanent_small_files"
            / "extracted"
            / "VMs"
            / f"{version}_setup_files"
            / "Runs"
            / fault
        )
    elif version == "v25p10":
        original_ll_statcords_source_path = (
            base_cybershake_dir
            / "setup_files_from_dropbox"
            / version
            / "permanent_small_files"
            / "extracted"
            / "fd_coords"
            / "Runs"
            / fault
        )
    else:
        raise ValueError(
            f"Don't know where to find .ll and .statcords files for version {version}"
        )

    ll_statcords_ll = original_ll_statcords_source_path / "fd_rt01-h0.100.ll"
    ll_statcords_statcords = (
        original_ll_statcords_source_path / "fd_rt01-h0.100.statcords"
    )

    if check_only:
        check_path(ll_statcords_ll, "fd .ll source", check_errors)
        check_path(ll_statcords_statcords, ".statcords source", check_errors)
    else:
        shutil.copy(ll_statcords_ll, destination_fault_params_base_base)
        shutil.copy(ll_statcords_statcords, destination_fault_params_base_base)
        print(f"    Copied to: {destination_fault_params_base_base}")

    print("[3/5] Moving Sources...")
    original_source_files_source_path = (
        base_cybershake_dir
        / "setup_files_from_dropbox"
        / version
        / "large_temp_files"
        / "extracted"
        / version
        / "Sources"
        / fault
        / fault
    )

    destination_source_files_path = (
        base_cybershake_dir / version / "Data" / "Sources" / fault
    )
    if check_only:
        check_path(
            original_source_files_source_path, "Sources source dir", check_errors
        )
    else:
        destination_source_files_path.parent.mkdir(parents=True, exist_ok=True)
        if not destination_source_files_path.exists():
            shutil.move(
                original_source_files_source_path, destination_source_files_path
            )
            print(f"    Moved to: {destination_source_files_path}")
        else:
            print(f"    Skipping Sources move (destination already exists)")

    destination_vms_base_dir = base_cybershake_dir / version / "Data" / "VMs" / fault

    print("[4/5] Moving VMs and updating vm_params.yaml...")

    if version == "v25p11":

        original_vm_meta_data_source_path = (
            base_cybershake_dir
            / "setup_files_from_dropbox"
            / version
            / "permanent_small_files"
            / "extracted"
            / "VMs"
            / "VMs_meta_data"
            / fault
        )

        original_vm_hdf5_source_path = (
            base_cybershake_dir
            / "setup_files_from_dropbox"
            / version
            / "large_temp_files"
            / "extracted"
            / version
            / "VMs"
            / "HDF5"
            / f"{fault}_velocity_model.h5"
        )

        if check_only:
            check_path(
                original_vm_meta_data_source_path,
                "VM metadata source dir",
                check_errors,
            )
            check_path(original_vm_hdf5_source_path, "VM HDF5 source", check_errors)
        else:
            destination_vms_base_dir.parent.mkdir(parents=True, exist_ok=True)
            if not destination_vms_base_dir.exists():
                shutil.move(original_vm_meta_data_source_path, destination_vms_base_dir)
            else:
                print(f"    Skipping VM metadata move (destination already exists)")

            hdf5_destination_path = destination_vms_base_dir

            hdf5_destination_file = (
                hdf5_destination_path / original_vm_hdf5_source_path.name
            )
            if not hdf5_destination_file.exists():
                shutil.move(original_vm_hdf5_source_path, hdf5_destination_file)
                print(
                    f"    Moved {original_vm_hdf5_source_path} to {hdf5_destination_file}"
                )
            else:
                print(f"    Skipping HDF5 move (destination already exists)")

            create_modified_config_file(
                original_file_path=hdf5_destination_path / "vm_params.yaml",
                modified_file_path=hdf5_destination_path / "vm_params.yaml",
                old_base_paths=["/scratch/hpc91a02/UC/RunFolder/Cybershake/v23p7"],
                new_base_path=base_cybershake_dir / version,
            )
            print(f"    Updated: {hdf5_destination_path / 'vm_params.yaml'}")

    elif version == "v25p10":
        vm_source_path = (
            base_cybershake_dir
            / "setup_files_from_dropbox"
            / version
            / "large_temp_files"
            / "extracted"
            / version
            / "VMs"
            / fault
            / fault
        )

        if check_only:
            check_path(vm_source_path, "VM source dir", check_errors)
        else:
            destination_vms_base_dir.parent.mkdir(parents=True, exist_ok=True)
            if not destination_vms_base_dir.exists():
                shutil.move(vm_source_path, destination_vms_base_dir)
                print(f"    Moved {vm_source_path} to {destination_vms_base_dir}")

            # Modify the vm_params.yaml file in place in the destination directory
            create_modified_config_file(
                original_file_path=destination_vms_base_dir / "vm_params.yaml",
                modified_file_path=destination_vms_base_dir / "vm_params.yaml",
                old_base_paths=[
                    "/scratch/hpc91a02/UC/RunFolder/Cybershake/v23p7",
                    "/scratch/hpc11a02/gmsim/RunFolder/Cybershake/v21p1",
                ],
                new_base_path=base_cybershake_dir / version,
            )
            print(f"    Updated: {destination_vms_base_dir / 'vm_params.yaml'}")

    else:
        raise ValueError(f"Unsupported version: {version}")

    # =============================================================================
    # Operations that depend on realization
    # =============================================================================
    print("[5/5] Processing realizations...")

    total_realizations = len(realizations)
    for idx, realization in enumerate(realizations, 1):
        print(f"  [{idx}/{total_realizations}] Processing {realization}...")

        if version == "v25p11":

            if fault == "AlpineF2K":
                lf_output_source_path = (
                    base_cybershake_dir
                    / "setup_files_from_dropbox"
                    / version
                    / "large_temp_files"
                    / "extracted"
                    / version
                    / "LF"
                    / fault
                    / fault
                    / f"{realization}_LF_OutBin"
                )

                lf_output_destination_path = (
                    base_cybershake_dir
                    / version
                    / "Runs"
                    / fault
                    / realization
                    / "LF"
                    / "OutBin"
                )

            else:
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
                    / fault
                    / realization
                    / "LF"
                )

                lf_output_destination_path = (
                    base_cybershake_dir / version / "Runs" / fault / realization / "LF"
                )

        elif version == "v25p10":
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

        else:
            raise ValueError(f"Unsupported version: {version}")

        # Create modified e3d.par file

        if version == "v25p11":
            original_e3d_par_file_path = (
                base_cybershake_dir
                / "setup_files_from_dropbox"
                / version
                / "permanent_small_files"
                / "extracted"
                / f"{version}_configs_params"
                / fault
                / realization
                / "LF"
                / "e3d.par"
            )
        elif version == "v25p10":
            original_e3d_par_file_path = lf_output_source_path / "e3d.par"
        else:
            raise ValueError(f"Unsupported version: {version}")

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

        if check_only:
            check_path(
                lf_output_source_path, f"{realization} LF source dir", check_errors
            )
            check_path(
                original_e3d_par_file_path,
                f"{realization} e3d.par source",
                check_errors,
            )
            check_path(
                original_sim_params_file_path,
                f"{realization} sim_params.yaml source",
                check_errors,
            )
        else:
            # Ensure parent directory exists before moving
            lf_output_destination_path.parent.mkdir(parents=True, exist_ok=True)

            if not lf_output_destination_path.exists():
                shutil.move(lf_output_source_path, lf_output_destination_path)
            else:
                print(f"    Skipping LF move (destination already exists)")

            create_modified_config_file(
                original_file_path=original_e3d_par_file_path,
                modified_file_path=modified_e3d_par_file_path,
                old_base_paths=old_base_paths_to_replace,
                new_base_path=base_cybershake_dir,
                fixed_value_overrides=E3D_PAR_FIXED_VALUES,
            )

            create_modified_config_file(
                original_file_path=original_sim_params_file_path,
                modified_file_path=modified_sim_params_file_path,
                old_base_paths=old_base_paths_to_replace,
                new_base_path=base_cybershake_dir,
            )

    print(f"\n{'='*60}")
    if check_only:
        if check_errors:
            print(f"Check FAILED: {len(check_errors)} missing path(s):")
            for error in check_errors:
                print(f"  - {error}")
            sys.exit(1)
        else:
            print("Check PASSED: all source files exist.")
    else:
        print("Deployment complete!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
