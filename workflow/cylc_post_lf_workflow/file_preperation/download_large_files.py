import argparse
import subprocess
import sys
from typing import Optional, Set

CATEGORIES = {"lf", "hf", "sources", "vm"}


def parse_categories(categories_arg: str, skip_arg: Optional[str] = None) -> Set[str]:
    """Parse category arguments into a set of categories to download."""
    if categories_arg == "all":
        selected = set(CATEGORIES)
    else:
        requested = {s.strip() for s in categories_arg.split(",") if s.strip()}
        invalid = requested - CATEGORIES
        if invalid:
            print(f"ERROR: Invalid category names: {', '.join(sorted(invalid))}")
            print(f"Valid categories: {', '.join(sorted(CATEGORIES))}")
            sys.exit(1)
        selected = requested

    if skip_arg:
        skip = {s.strip() for s in skip_arg.split(",") if s.strip()}
        invalid = skip - CATEGORIES
        if invalid:
            print(
                f"ERROR: Invalid category names in --skip: {', '.join(sorted(invalid))}"
            )
            print(f"Valid categories: {', '.join(sorted(CATEGORIES))}")
            sys.exit(1)
        selected -= skip

    return selected


def main():
    parser = argparse.ArgumentParser(
        description="Download large files from Dropbox using rclone",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Categories: lf, hf, sources, vm

Source tar naming by version:
    v25p11 -> {fault_name}.tar
    v25p10 -> {fault_name}_Sources.tar

Examples:
  # Download all categories (default)
  %(prog)s v25p11 WhiteCk

  # Download only HF files
  %(prog)s v25p11 WhiteCk --categories hf

  # Download LF and sources
  %(prog)s v25p11 WhiteCk --categories lf,sources

    # Download sources for v25p10 (uses HopeCW_Sources.tar)
    %(prog)s v25p10 HopeCW --categories sources

  # Download all except VM
  %(prog)s v25p11 WhiteCk --skip vm
        """,
    )
    parser.add_argument(
        "version",
        choices=["v25p10", "v25p11"],
        help="Version identifier (must be v25p10 or v25p11)",
    )
    parser.add_argument("fault_name", help="Fault name (e.g., WhiteCk)")
    parser.add_argument(
        "--categories",
        default="all",
        help="Comma-separated list of categories to download: lf, hf, sources, vm (default: all)",
    )
    parser.add_argument(
        "--skip",
        default=None,
        help="Comma-separated list of categories to skip",
    )
    args = parser.parse_args()

    version = args.version
    fault_name = args.fault_name
    selected = parse_categories(args.categories, args.skip)

    if not selected:
        print("No categories selected to download.")
        sys.exit(0)

    print(f"Downloading categories: {', '.join(sorted(selected))}")

    dropbox_source_base = f"dropbox:/QuakeCoRE/gmsim_scratch/{version}"
    dropbox_lf = f"{dropbox_source_base}/LF/{fault_name}"
    dropbox_hf = f"{dropbox_source_base}/HF/{fault_name}"
    dropbox_sources_tar = None
    if "sources" in selected:
        if version == "v25p11":
            dropbox_sources_tar = f"{dropbox_source_base}/Sources/{fault_name}.tar"
        else:
            dropbox_sources_tar = (
                f"{dropbox_source_base}/Sources/{fault_name}_Sources.tar"
            )
    dropbox_vm_h5 = f"{dropbox_source_base}/VMs/HDF5/{fault_name}_velocity_model.h5"

    local_large_temp_tar_dir_base = f"/scratch/projects/rch-quakecore/Cybershake/setup_files_from_dropbox/{version}/large_temp_files/tar/{version}"
    local_lf_tar_dir = f"{local_large_temp_tar_dir_base}/LF/{fault_name}"
    local_hf_tar_dir = f"{local_large_temp_tar_dir_base}/HF/{fault_name}"
    local_sources_tar_dir = f"{local_large_temp_tar_dir_base}/Sources"
    local_vm_h5_dir = f"{local_large_temp_tar_dir_base}/VMs/HDF5"

    if "lf" in selected:
        print(f"Trying to clone {dropbox_lf} to {local_lf_tar_dir}")
        subprocess.run(
            ["rclone", "copy", dropbox_lf, local_lf_tar_dir, "--progress"], check=True
        )

    if "hf" in selected:
        print(f"Trying to clone {dropbox_hf} to {local_hf_tar_dir}")
        subprocess.run(
            ["rclone", "copy", dropbox_hf, local_hf_tar_dir, "--progress"], check=True
        )

    if "sources" in selected:
        print(f"Trying to clone {dropbox_sources_tar} to {local_sources_tar_dir}")
        subprocess.run(
            [
                "rclone",
                "copy",
                dropbox_sources_tar,
                local_sources_tar_dir,
                "--progress",
            ],
            check=True,
        )

    if "vm" in selected:
        print(f"Trying to clone {dropbox_vm_h5} to {local_vm_h5_dir}")
        subprocess.run(
            ["rclone", "copy", dropbox_vm_h5, local_vm_h5_dir, "--progress"], check=True
        )


if __name__ == "__main__":
    main()
