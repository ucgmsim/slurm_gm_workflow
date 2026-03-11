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
from typing import Callable, Dict, List, Set, Optional
from natsort import natsorted


# =============================================================================
# Stage Configuration
# =============================================================================


class StageInfo:
    """Information about a deployment stage."""

    def __init__(
        self, name: str, description: str, scope: str, func: Optional[Callable] = None
    ):
        self.name = name
        self.description = description
        self.scope = scope  # 'version', 'fault', or 'realization'
        self.func = func


# Stage registry - will be populated with functions after they're defined
STAGES: Dict[str, StageInfo] = {}


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


def check_non_empty_directory(path, description, errors):
    """Check that a directory exists and contains at least one entry."""
    if not path.exists():
        print(f"    MISSING {description}: {path}")
        errors.append(f"{description}: {path}")
        return

    if not path.is_dir():
        print(f"    NOT A DIRECTORY {description}: {path}")
        errors.append(f"{description} is not a directory: {path}")
        return

    try:
        next(path.iterdir())
    except StopIteration:
        print(f"    EMPTY {description}: {path}")
        errors.append(f"{description} is empty: {path}")


def cleanup_empty_directories(root_dir: Path):
    """
    Remove empty directories under root_dir by walking bottom-up.

    After files are moved out of the staging area, the parent directories
    are left behind empty. This walks the tree from the leaves upward,
    removing any directory that is empty (or becomes empty after its
    children are removed). Stops at root_dir itself (does not remove it).
    """
    if not root_dir.exists():
        return

    removed_count = 0
    # Walk bottom-up so children are processed before parents
    for dirpath, dirnames, filenames in os.walk(str(root_dir), topdown=False):
        dir_path = Path(dirpath)
        if dir_path == root_dir:
            continue
        try:
            if not any(dir_path.iterdir()):
                dir_path.rmdir()
                removed_count += 1
        except OSError:
            pass

    if removed_count > 0:
        print(f"  Cleaned up {removed_count} empty director{'y' if removed_count == 1 else 'ies'} under {root_dir}")


# =============================================================================
# Stage Deployment Functions
# =============================================================================


def deploy_root_params(
    version: str,
    fault: str,
    check_only: bool,
    check_errors: list,
    base_cybershake_dir: Path,
    old_base_paths_to_replace: list,
    **kwargs,
) -> bool:
    """Deploy root_params.yaml file (version-level)."""
    print("  [root_params] Creating modified root_params.yaml file...")

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
        print(f"    Created: {modified_root_params_file_path}")

    return True


def deploy_ll_vs30(
    version: str,
    fault: str,
    check_only: bool,
    check_errors: list,
    base_cybershake_dir: Path,
    ll_and_vs30_files_provided_in_this_version: str,
    **kwargs,
) -> bool:
    """Deploy .ll and .vs30 files (version-level)."""
    print("  [ll_vs30] Copying .ll and .vs30 files...")

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
        print(f"    Copied to: {destination_ll_and_vs30_path}")

    return True


def deploy_fault_params(
    version: str,
    fault: str,
    check_only: bool,
    check_errors: list,
    base_cybershake_dir: Path,
    old_base_paths_to_replace: list,
    **kwargs,
) -> bool:
    """Deploy fault_params.yaml file (fault-level)."""
    print("  [fault_params] Creating modified fault_params.yaml file...")

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

    return True


def deploy_ll_statcords(
    version: str,
    fault: str,
    check_only: bool,
    check_errors: list,
    base_cybershake_dir: Path,
    **kwargs,
) -> bool:
    """Deploy .ll and .statcords files (fault-level)."""
    print("  [ll_statcords] Copying .ll and .statcords files...")

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

    destination_fault_params_base_base = base_cybershake_dir / version / "Runs" / fault

    if check_only:
        check_path(ll_statcords_ll, "fd .ll source", check_errors)
        check_path(ll_statcords_statcords, ".statcords source", check_errors)
    else:
        shutil.copy(ll_statcords_ll, destination_fault_params_base_base)
        shutil.copy(ll_statcords_statcords, destination_fault_params_base_base)
        print(f"    Copied to: {destination_fault_params_base_base}")

    return True


def deploy_sources(
    version: str,
    fault: str,
    check_only: bool,
    check_errors: list,
    base_cybershake_dir: Path,
    **kwargs,
) -> bool:
    """Deploy Sources directory (fault-level)."""
    print("  [sources] Moving Sources...")

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
        check_non_empty_directory(
            original_source_files_source_path,
            "Sources source dir",
            check_errors,
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

    return True


def deploy_vms(
    version: str,
    fault: str,
    check_only: bool,
    check_errors: list,
    base_cybershake_dir: Path,
    **kwargs,
) -> bool:
    """Deploy VMs and update vm_params.yaml (fault-level)."""
    print("  [vms] Moving VMs and updating vm_params.yaml...")

    destination_vms_base_dir = base_cybershake_dir / version / "Data" / "VMs" / fault

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

        original_vm_params_path = original_vm_meta_data_source_path / "vm_params.yaml"

        if check_only:
            check_non_empty_directory(
                original_vm_meta_data_source_path,
                "VM metadata source dir",
                check_errors,
            )
            check_path(original_vm_hdf5_source_path, "VM HDF5 source", check_errors)
            check_path(original_vm_params_path, "vm_params.yaml source", check_errors)
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

        vm_params_path = vm_source_path / "vm_params.yaml"

        if check_only:
            check_non_empty_directory(vm_source_path, "VM source dir", check_errors)
            check_path(vm_params_path, "vm_params.yaml source", check_errors)
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

    return True


def deploy_realizations_lf(
    version: str,
    fault: str,
    check_only: bool,
    check_errors: list,
    base_cybershake_dir: Path,
    old_base_paths_to_replace: list,
    realizations: list,
    E3D_PAR_FIXED_VALUES: dict,
    **kwargs,
) -> bool:
    """Deploy LF realizations (realization-level)."""
    print("  [realization_lf] Processing LF for realizations...")

    total_realizations = len(realizations)
    for idx, realization in enumerate(realizations, 1):
        print(f"    [{idx}/{total_realizations}] Processing LF for {realization}...")

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

        # Get e3d.par file path
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

        # Get sim_params.yaml file path
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
            check_non_empty_directory(
                lf_output_source_path,
                f"{realization} LF source dir",
                check_errors,
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
                print(f"      Skipping LF move (destination already exists)")

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

    return True


def deploy_realizations_hf(
    version: str,
    fault: str,
    check_only: bool,
    check_errors: list,
    base_cybershake_dir: Path,
    realizations: list,
    **kwargs,
) -> bool:
    """Deploy HF realizations (realization-level)."""
    print("  [realization_hf] Processing HF for realizations...")

    total_realizations = len(realizations)
    for idx, realization in enumerate(realizations, 1):
        print(f"    [{idx}/{total_realizations}] Processing HF for {realization}...")

        # Construct source path
        # Pattern: .../HF/{fault}/{realization}_HF/Runs/{fault}/{realization}/HF/Acc
        # Note: realization names already include the fault prefix (e.g. HopeTARA_REL01)
        hf_output_source_path = (
            base_cybershake_dir
            / "setup_files_from_dropbox"
            / version
            / "large_temp_files"
            / "extracted"
            / version
            / "HF"
            / fault
            / f"{realization}_HF"
            / "Runs"
            / fault
            / realization
            / "HF"
            / "Acc"
        )

        # Construct destination path
        # Pattern: .../{version}/Runs/{fault}/{realization}/HF/Acc
        hf_output_destination_path = (
            base_cybershake_dir / version / "Runs" / fault / realization / "HF" / "Acc"
        )

        if check_only:
            check_non_empty_directory(
                hf_output_source_path,
                f"{realization} HF source dir",
                check_errors,
            )
        else:
            # Ensure parent directory exists before moving
            hf_output_destination_path.parent.mkdir(parents=True, exist_ok=True)

            if not hf_output_destination_path.exists():
                shutil.move(hf_output_source_path, hf_output_destination_path)
            else:
                print(f"      Skipping HF move (destination already exists)")

    return True


# Register all stages
STAGES = {
    "root_params": StageInfo(
        "root_params", "Create modified root_params.yaml", "version", deploy_root_params
    ),
    "ll_vs30": StageInfo(
        "ll_vs30",
        "Copy .ll and .vs30 files to version directory",
        "version",
        deploy_ll_vs30,
    ),
    "fault_params": StageInfo(
        "fault_params",
        "Create modified fault_params.yaml",
        "fault",
        deploy_fault_params,
    ),
    "ll_statcords": StageInfo(
        "ll_statcords", "Copy fd .ll and .statcords files", "fault", deploy_ll_statcords
    ),
    "sources": StageInfo("sources", "Move Sources directory", "fault", deploy_sources),
    "vms": StageInfo("vms", "Move VMs and update vm_params.yaml", "fault", deploy_vms),
    "realization_lf": StageInfo(
        "realization_lf",
        "Process LF for all realizations",
        "realization",
        deploy_realizations_lf,
    ),
    "realization_hf": StageInfo(
        "realization_hf",
        "Process HF for all realizations",
        "realization",
        deploy_realizations_hf,
    ),
}


# Natural execution order for stages (respects dependencies)
STAGE_ORDER = [
    "root_params",
    "ll_vs30",
    "fault_params",
    "ll_statcords",
    "sources",
    "vms",
    "realization_lf",
    "realization_hf",
]


def parse_stages(stages_arg: str, skip_stages_arg: Optional[str] = None) -> Set[str]:
    """
    Parse stage arguments into a set of stages to run.

    Parameters
    ----------
    stages_arg : str
        Comma-separated list of stages, or "all"
    skip_stages_arg : str, optional
        Comma-separated list of stages to skip

    Returns
    -------
    Set[str]
        Set of stage names to execute
    """
    # Parse stages to include
    if stages_arg == "all":
        selected_stages = set(STAGES.keys())
    else:
        requested = {s.strip() for s in stages_arg.split(",") if s.strip()}

        # Validate stage names
        invalid = requested - STAGES.keys()
        if invalid:
            print(f"ERROR: Invalid stage names: {', '.join(sorted(invalid))}")
            print(f"Valid stages: {', '.join(sorted(STAGES.keys()))}")
            sys.exit(1)

        selected_stages = requested

    # Apply skip filter
    if skip_stages_arg:
        skip_stages = {s.strip() for s in skip_stages_arg.split(",") if s.strip()}

        # Validate skip stage names
        invalid = skip_stages - STAGES.keys()
        if invalid:
            print(
                f"ERROR: Invalid stage names in --skip-stages: {', '.join(sorted(invalid))}"
            )
            print(f"Valid stages: {', '.join(sorted(STAGES.keys()))}")
            sys.exit(1)

        selected_stages -= skip_stages

    return selected_stages


def list_stages():
    """Print all available stages and exit."""
    print("\n" + "=" * 60)
    print("Available Deployment Stages")
    print("=" * 60 + "\n")

    # Group by scope
    version_stages = [s for s in STAGES.values() if s.scope == "version"]
    fault_stages = [s for s in STAGES.values() if s.scope == "fault"]
    realization_stages = [s for s in STAGES.values() if s.scope == "realization"]

    if version_stages:
        print("VERSION-LEVEL (run once per version, affects all faults):")
        for stage in version_stages:
            print(f"  {stage.name:20s} - {stage.description}")
        print()

    if fault_stages:
        print("FAULT-LEVEL (run once per fault):")
        for stage in fault_stages:
            print(f"  {stage.name:20s} - {stage.description}")
        print()

    if realization_stages:
        print("REALIZATION-LEVEL (run for each realization):")
        for stage in realization_stages:
            print(f"  {stage.name:20s} - {stage.description}")
        print()

    print("=" * 60)
    print("Usage Examples:")
    print("  # Run all stages (default)")
    print("  python deploy_files_for_post_lf_stages.py v25p11 NMFZB1")
    print()
    print("  # Run only specific stages")
    print(
        "  python deploy_files_for_post_lf_stages.py v25p11 NMFZB1 --stages fault_params,sources"
    )
    print()
    print("  # Skip specific stages")
    print(
        "  python deploy_files_for_post_lf_stages.py v25p11 NMFZB1 --skip-stages ll_statcords"
    )
    print()
    print("  # Check mode (validate sources without deploying)")
    print("  python deploy_files_for_post_lf_stages.py v25p11 NMFZB1 --check")
    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Deploy files for post-LF stages of the Cybershake workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Stage Control Examples:
  # Run all stages (default)
  %(prog)s v25p11 NMFZB1
  
  # Run only specific stages
  %(prog)s v25p11 NMFZB1 --stages fault_params,sources,vms
  
  # Skip specific stages
  %(prog)s v25p11 NMFZB1 --skip-stages ll_statcords
  
  # Run only LF realization processing
  %(prog)s v25p11 NMFZB1 --stages realization_lf
  
  # Check mode (validate sources exist)
  %(prog)s v25p11 NMFZB1 --check
  
  # List all available stages
  %(prog)s --list-stages
        """,
    )
    parser.add_argument("version", nargs="?", help="Version string (e.g., v25p11)")
    parser.add_argument("fault", nargs="?", help="Fault name (e.g., NMFZB1)")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only check that source files exist, without deploying anything",
    )
    parser.add_argument(
        "--stages",
        default="all",
        help="Comma-separated list of stages to run (default: all). Use --list-stages to see available stages.",
    )
    parser.add_argument(
        "--skip-stages",
        default=None,
        help="Comma-separated list of stages to skip",
    )
    parser.add_argument(
        "--list-stages",
        action="store_true",
        help="List all available stages and exit",
    )
    args = parser.parse_args()

    # Handle --list-stages
    if args.list_stages:
        list_stages()
        sys.exit(0)

    # Validate required arguments
    if not args.version or not args.fault:
        parser.error("version and fault are required unless using --list-stages")

    version = args.version
    fault = args.fault
    check_only = args.check
    check_errors = []

    # Parse which stages to run
    selected_stages = parse_stages(args.stages, args.skip_stages)

    # Filter to natural execution order
    stages_to_run = [s for s in STAGE_ORDER if s in selected_stages]

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

    ll_and_vs30_files_provided_in_this_version = "v25p11"

    # Get realizations list if needed
    realizations = []
    if any(STAGES[s].scope == "realization" for s in stages_to_run):
        realizations_dir = (
            base_cybershake_dir
            / "setup_files_from_dropbox"
            / version
            / "permanent_small_files"
            / "extracted"
            / f"{version}_configs_params"
            / fault
        )
        try:
            realizations = natsorted(
                [
                    d
                    for d in os.listdir(realizations_dir)
                    if os.path.isdir(realizations_dir / d)
                ]
            )
        except FileNotFoundError:
            print(f"ERROR: Realizations directory not found: {realizations_dir}")
            print(f"Cannot run realization-level stages without valid fault directory.")
            sys.exit(1)

    # Print header
    mode_label = "Checking" if check_only else "Deploying"
    print(f"\n{'='*60}")
    print(f"{mode_label} files for version={version}, fault={fault}")
    if realizations:
        print(f"Total realizations to process: {len(realizations)}")
    print(f"Stages to run: {', '.join(stages_to_run) if stages_to_run else 'NONE'}")
    if args.skip_stages:
        skipped = selected_stages ^ set(stages_to_run)
        if skipped:
            print(f"Stages skipped: {', '.join(sorted(skipped))}")
    print(f"{'='*60}\n")

    if not stages_to_run:
        print("No stages selected to run. Use --list-stages to see available stages.")
        sys.exit(0)

    # Prepare kwargs for stage functions
    stage_kwargs = {
        "version": version,
        "fault": fault,
        "check_only": check_only,
        "check_errors": check_errors,
        "base_cybershake_dir": base_cybershake_dir,
        "old_base_paths_to_replace": old_base_paths_to_replace,
        "ll_and_vs30_files_provided_in_this_version": ll_and_vs30_files_provided_in_this_version,
        "realizations": realizations,
        "E3D_PAR_FIXED_VALUES": E3D_PAR_FIXED_VALUES,
    }

    # Execute stages
    for stage_name in stages_to_run:
        stage = STAGES[stage_name]
        try:
            success = stage.func(**stage_kwargs)
            if not success:
                print(f"  WARNING: Stage {stage_name} returned failure")
        except Exception as e:
            print(f"  ERROR in stage {stage_name}: {e}")
            if not check_only:
                raise

    # Clean up empty directories left behind after moves
    if not check_only:
        staging_root = (
            base_cybershake_dir
            / "setup_files_from_dropbox"
            / version
            / "large_temp_files"
            / "extracted"
        )
        cleanup_empty_directories(staging_root)

    # Print summary
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
