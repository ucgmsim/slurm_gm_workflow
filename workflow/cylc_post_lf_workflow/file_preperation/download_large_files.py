import argparse
import subprocess
import sys
from typing import List, Optional, Set

CATEGORIES = {"lf", "hf", "sources", "vm"}


def parse_file_list(files_arg: Optional[str]) -> Optional[List[str]]:
    """Parse a comma-separated file list into a list of filenames, or None if not provided."""
    if files_arg is None:
        return None
    return [f.strip() for f in files_arg.split(",") if f.strip()]


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

  # Download specific LF files only
  %(prog)s v25p11 WhiteCk --categories lf --lf-files WhiteCk_REL01.tar

  # Download specific HF files only
  %(prog)s v25p11 WhiteCk --categories hf --hf-files WhiteCk_REL01.bin,WhiteCk_REL02.bin

  # Mix: specific LF files and all HF files
  %(prog)s v25p11 WhiteCk --categories lf,hf --lf-files WhiteCk_REL01.tar
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
    parser.add_argument(
        "--lf-files",
        default=None,
        dest="lf_files",
        help="Comma-separated list of specific filenames to download from the LF directory (default: download entire directory)",
    )
    parser.add_argument(
        "--hf-files",
        default=None,
        dest="hf_files",
        help="Comma-separated list of specific filenames to download from the HF directory (default: download entire directory)",
    )
    args = parser.parse_args()

    version = args.version
    fault_name = args.fault_name
    selected = parse_categories(args.categories, args.skip)
    lf_files = parse_file_list(args.lf_files)
    hf_files = parse_file_list(args.hf_files)

    if not selected:
        print("No categories selected to download.")
        sys.exit(0)

    print(f"Downloading categories: {', '.join(sorted(selected))}")

    dropbox_source_base = f"dropbox:/QuakeCoRE/gmsim_scratch/{version}"
    dropbox_lf = f"{dropbox_source_base}/LF/{fault_name}"
    if version == "v25p11" and fault_name == "AlpineF2K":
        dropbox_hf = f"dropbox:/QuakeCoRE/gmsim_scratch/v25p11/HF_nesi"
    else:
        dropbox_hf = f"{dropbox_source_base}/HF/{fault_name}"
    dropbox_sources_tar = None
    if "sources" in selected:
        if version == "v25p11":
            dropbox_sources_tar = (
                f"{dropbox_source_base}/Sources/{fault_name}_Source.tar"
            )
        else:
            dropbox_sources_tar = (
                f"{dropbox_source_base}/Sources/{fault_name}_Sources.tar"
            )
    if version == "v25p11":
        dropbox_vm_file = (
            f"{dropbox_source_base}/VMs/HDF5/{fault_name}_velocity_model.h5"
        )
    else:
        dropbox_vm_file = f"{dropbox_source_base}/VMs/{fault_name}_VM.tar"

    local_large_temp_tar_dir_base = f"/scratch/projects/rch-quakecore/Cybershake/setup_files_from_dropbox/{version}/large_temp_files/tar/{version}"
    local_lf_tar_dir = f"{local_large_temp_tar_dir_base}/LF/{fault_name}"
    local_hf_tar_dir = f"{local_large_temp_tar_dir_base}/HF/{fault_name}"
    local_sources_tar_dir = f"{local_large_temp_tar_dir_base}/Sources"
    if version == "v25p11":
        local_vm_dir = f"{local_large_temp_tar_dir_base}/VMs/HDF5"
    else:
        local_vm_dir = f"{local_large_temp_tar_dir_base}/VMs"

    if "lf" in selected:
        if lf_files is None:
            print(f"Trying to clone {dropbox_lf} to {local_lf_tar_dir}")
            subprocess.run(
                ["rclone", "copy", dropbox_lf, local_lf_tar_dir, "--progress"],
                check=True,
            )
        else:
            for filename in lf_files:
                src = f"{dropbox_lf}/{filename}"
                print(f"Trying to clone {src} to {local_lf_tar_dir}")
                subprocess.run(
                    ["rclone", "copy", src, local_lf_tar_dir, "--progress"],
                    check=True,
                )

    if "hf" in selected:
        if hf_files is None:
            print(f"Trying to clone {dropbox_hf} to {local_hf_tar_dir}")
            subprocess.run(
                ["rclone", "copy", dropbox_hf, local_hf_tar_dir, "--progress"],
                check=True,
            )
        else:
            for filename in hf_files:
                src = f"{dropbox_hf}/{filename}"
                print(f"Trying to clone {src} to {local_hf_tar_dir}")
                subprocess.run(
                    ["rclone", "copy", src, local_hf_tar_dir, "--progress"],
                    check=True,
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
        print(f"Trying to clone {dropbox_vm_file} to {local_vm_dir}")
        subprocess.run(
            ["rclone", "copy", dropbox_vm_file, local_vm_dir, "--progress"], check=True
        )


if __name__ == "__main__":
    main()
