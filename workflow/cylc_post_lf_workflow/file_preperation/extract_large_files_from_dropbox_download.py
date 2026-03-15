#!/usr/bin/env python3
"""
Script to extract all compressed files from tar_original_setup_files_from_dropbox
and create an extracted version under extracted_original_setup_files_from_dropbox.

Handles nested archives (compressed files within compressed files) by recursively
extracting until no compressed files remain.
"""

import argparse
import shutil
import sys
import tarfile
import zipfile
import gzip
import bz2
import lzma
from pathlib import Path
from typing import Optional, Set

CATEGORIES = {"lf", "hf", "sources", "vm"}


def parse_categories(categories_arg: str, skip_arg: Optional[str] = None) -> Set[str]:
    """Parse category arguments into a set of categories to extract."""
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
            print(f"ERROR: Invalid category names in --skip: {', '.join(sorted(invalid))}")
            print(f"Valid categories: {', '.join(sorted(CATEGORIES))}")
            sys.exit(1)
        selected -= skip

    return selected

# File extensions that indicate compressed/archived files
TAR_EXTENSIONS = {".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz"}
COMPRESSED_EXTENSIONS = {".gz", ".bz2", ".xz", ".zip"}
ALL_ARCHIVE_EXTENSIONS = TAR_EXTENSIONS | COMPRESSED_EXTENSIONS


def get_archive_type(filepath: Path) -> str | None:
    """
    Determine the archive type based on file extension.
    Returns 'tar', 'zip', 'gz', 'bz2', 'xz', or None if not an archive.
    """
    name = filepath.name.lower()

    # Check for tar archives (including compressed tars)
    if name.endswith(".tar.gz") or name.endswith(".tgz"):
        return "tar.gz"
    elif name.endswith(".tar.bz2") or name.endswith(".tbz2"):
        return "tar.bz2"
    elif name.endswith(".tar.xz") or name.endswith(".txz"):
        return "tar.xz"
    elif name.endswith(".tar"):
        return "tar"
    elif name.endswith(".zip"):
        return "zip"
    elif name.endswith(".gz"):
        return "gz"
    elif name.endswith(".bz2"):
        return "bz2"
    elif name.endswith(".xz"):
        return "xz"

    return None


def get_extracted_name(filepath: Path, archive_type: str) -> str:
    """
    Get the name of the file/directory after extraction.
    For tar archives, returns the stem without the archive extension.
    For single-file compression (gz, bz2, xz), removes the compression extension.
    """
    name = filepath.name

    if archive_type in ("tar.gz", "tar.bz2", "tar.xz"):
        # Remove both extensions (e.g., .tar.gz)
        if name.endswith((".tar.gz", ".tar.bz2", ".tar.xz")):
            return name.rsplit(".tar.", 1)[0]
        elif name.endswith((".tgz", ".tbz2", ".txz")):
            return name.rsplit(".", 1)[0]
    elif archive_type == "tar":
        return name.rsplit(".tar", 1)[0]
    elif archive_type == "zip":
        return name.rsplit(".zip", 1)[0]
    elif archive_type in ("gz", "bz2", "xz"):
        # For single-file compression, just remove the extension
        return name.rsplit(".", 1)[0]

    return name


def extract_tar(filepath: Path, dest_dir: Path) -> Path:
    """
    Extract a tar archive (optionally compressed) to destination directory.
    Handles symlinks and other special files by extracting what we can and ignoring errors.
    """
    with tarfile.open(filepath, "r:*") as tar:
        for member in tar.getmembers():
            try:
                tar.extract(member, dest_dir)
            except (OSError, tarfile.TarError) as e:
                # Skip files that can't be extracted (broken symlinks, permission issues, etc.)
                print(f"      Skipping member {member.name}: {e}")
    return dest_dir


def extract_zip(filepath: Path, dest_dir: Path) -> Path:
    """Extract a zip archive to destination directory."""
    with zipfile.ZipFile(filepath, "r") as zf:
        zf.extractall(dest_dir)
    return dest_dir


def extract_single_compressed(
    filepath: Path, dest_dir: Path, archive_type: str
) -> Path:
    """
    Extract a single-file compressed file (gz, bz2, xz).
    Returns the path to the extracted file.
    """
    extracted_name = get_extracted_name(filepath, archive_type)
    dest_file = dest_dir / extracted_name

    if archive_type == "gz":
        with gzip.open(filepath, "rb") as f_in:
            with open(dest_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
    elif archive_type == "bz2":
        with bz2.open(filepath, "rb") as f_in:
            with open(dest_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
    elif archive_type == "xz":
        with lzma.open(filepath, "rb") as f_in:
            with open(dest_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

    return dest_file


def extract_archive(filepath: Path, dest_dir: Path) -> Path | None:
    """
    Extract an archive file to the destination directory.
    Returns the path where contents were extracted, or None if not an archive.
    """
    archive_type = get_archive_type(filepath)

    if archive_type is None:
        return None

    dest_dir.mkdir(parents=True, exist_ok=True)

    if archive_type.startswith("tar") or archive_type == "tar":
        return extract_tar(filepath, dest_dir)
    elif archive_type == "zip":
        return extract_zip(filepath, dest_dir)
    elif archive_type in ("gz", "bz2", "xz"):
        return extract_single_compressed(filepath, dest_dir, archive_type)

    return None


def find_archives_in_directory(directory: Path) -> list[Path]:
    """Find all archive files in a directory (recursively)."""
    archives = []
    for filepath in directory.rglob("*"):
        if filepath.is_file() and get_archive_type(filepath) is not None:
            archives.append(filepath)
    return archives


def extract_recursively(directory: Path, max_depth: int = 10) -> None:
    """
    Recursively extract all archives in a directory until no archives remain.
    max_depth prevents infinite loops in case of issues.
    """
    depth = 0
    while depth < max_depth:
        archives = find_archives_in_directory(directory)
        if not archives:
            print(f"  No more archives found after {depth} extraction passes")
            break

        print(f"  Pass {depth + 1}: Found {len(archives)} archive(s) to extract")

        for archive_path in archives:
            archive_type = get_archive_type(archive_path)

            # Determine extraction destination
            if archive_type in ("gz", "bz2", "xz"):
                # Single-file compression: extract to same directory
                extract_dest = archive_path.parent
            elif archive_type not in ("gz", "bz2", "xz") and archive_type is not None:
                # Multi-file archives: extract to a subdirectory with the archive name
                extracted_name = get_extracted_name(archive_path, archive_type)
                extract_dest = archive_path.parent / extracted_name
            else:
                raise ValueError(f"Unsupported archive type for file {archive_path}")

            print(f"    Extracting: {archive_path.relative_to(directory)}")

            try:
                extract_archive(archive_path, extract_dest)
            except Exception as e:
                print(f"    WARNING: Error during extraction of {archive_path}: {e}")
            finally:
                # Always remove the archive after attempting extraction
                # (even if extraction had errors, we extracted what we could)
                try:
                    archive_path.unlink()
                except Exception as e:
                    print(f"    WARNING: Could not delete archive {archive_path}: {e}")

        depth += 1

    if depth >= max_depth:
        print(f"  WARNING: Reached maximum extraction depth ({max_depth})")


def remove_broken_symlinks(directory: Path) -> None:
    """
    Find and remove all broken symlinks (symlinks pointing to non-existent paths)
    in the given directory tree.
    """
    broken_links = []
    for filepath in directory.rglob("*"):
        if filepath.is_symlink() and not filepath.exists():
            broken_links.append(filepath)

    if broken_links:
        print(f"Removing {len(broken_links)} broken symlink(s)...")
        for link in broken_links:
            print(f"    Removing broken symlink: {link.relative_to(directory)}")
            try:
                link.unlink()
            except Exception as e:
                print(f"    WARNING: Could not remove symlink {link}: {e}")
    else:
        print("No broken symlinks found.")


def process_directory_tree(src_path: Path, dest_dir: Path) -> None:
    """
    Process the source path (file or directory), extracting archives.

    - If src_path is a directory: moves it to dest_dir, then recursively extracts all archives
    - If src_path is an archive file: extracts it to dest_dir, then recursively extracts nested archives
    """
    if not src_path.exists():
        raise FileNotFoundError(f"Source path does not exist: {src_path}")

    if src_path.is_file():
        # Handle single archive file
        archive_type = get_archive_type(src_path)
        if archive_type is None:
            raise ValueError(f"Source file is not a recognized archive: {src_path}")

        print(f"Extracting archive file {src_path} to {dest_dir}")
        dest_dir.mkdir(parents=True, exist_ok=True)
        extract_archive(src_path, dest_dir)
    else:
        # Handle directory tree
        print(f"Moving directory tree from {src_path} to {dest_dir}")
        dest_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(src_path, dest_dir)

    # Now recursively extract all archives
    print("Recursively extracting all archives...")
    extract_recursively(dest_dir)

    # Remove any broken symlinks
    print("Checking for broken symlinks...")
    remove_broken_symlinks(dest_dir)

    print("Done!")


def main():
    parser = argparse.ArgumentParser(
        description="Extract compressed files from Dropbox download for a specific version and fault",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Categories: lf, hf, sources, vm

Examples:
  # Extract all categories (default)
  %(prog)s v25p11 WhiteCk

  # Extract only HF files
  %(prog)s v25p11 WhiteCk --categories hf

  # Extract LF and sources
  %(prog)s v25p11 WhiteCk --categories lf,sources

  # Extract all except VM
  %(prog)s v25p11 WhiteCk --skip vm
        """,
    )
    parser.add_argument("version", help="Version string (e.g., v25p11)")
    parser.add_argument("fault", help="Fault name (e.g., WhiteCk)")
    parser.add_argument(
        "--categories",
        default="all",
        help="Comma-separated list of categories to extract: lf, hf, sources, vm (default: all)",
    )
    parser.add_argument(
        "--skip",
        default=None,
        help="Comma-separated list of categories to skip",
    )
    args = parser.parse_args()

    selected = parse_categories(args.categories, args.skip)

    if not selected:
        print("No categories selected to extract.")
        sys.exit(0)

    # Construct paths using the provided version and fault
    setup_files_base_dir = Path(
        f"/scratch/projects/rch-quakecore/Cybershake/setup_files_from_dropbox/{args.version}/large_temp_files"
    )
    tar_original_setup_files_from_dropbox = setup_files_base_dir / "tar" / args.version
    extracted_original_setup_files_from_dropbox = (
        setup_files_base_dir / "extracted" / args.version
    )

    print(f"Processing version: {args.version}, fault: {args.fault}")
    print(f"Extracting categories: {', '.join(sorted(selected))}")

    # if "lf" in selected:
    #     print(f"Move and extract LF dir for fault {args.fault}...")
    #     process_directory_tree(
    #         tar_original_setup_files_from_dropbox / "LF" / args.fault,
    #         extracted_original_setup_files_from_dropbox / "LF" / args.fault,
    #     )

    # if "hf" in selected:
    #     print(f"Move and extract HF dir for fault {args.fault}...")
    #     process_directory_tree(
    #         tar_original_setup_files_from_dropbox / "HF" / args.fault,
    #         extracted_original_setup_files_from_dropbox / "HF" / args.fault,
    #     )

    if "sources" in selected:
        print(f"Move and extract Sources dir for fault {args.fault}...")
        if args.version == "v25p10":
            process_directory_tree(
                tar_original_setup_files_from_dropbox
                / "Sources"
                / f"{args.fault}_Sources.tar",
                extracted_original_setup_files_from_dropbox / "Sources" / f"{args.fault}",
            )
        elif args.version == "v25p11":
            process_directory_tree(
                tar_original_setup_files_from_dropbox / "Sources" / f"{args.fault}_Source.tar",
                extracted_original_setup_files_from_dropbox / "Sources" / f"{args.fault}",
            )
        else:
            raise ValueError(f"Unsupported version: {args.version}")

    if "vm" in selected:
        if args.version == "v25p10":
            print(f"Extract VM files for fault {args.fault}...")
            process_directory_tree(
                tar_original_setup_files_from_dropbox / "VMs" / f"{args.fault}_VM.tar",
                extracted_original_setup_files_from_dropbox / "VMs" / f"{args.fault}",
            )
        elif args.version == "v25p11":
            dest_vm_dir = extracted_original_setup_files_from_dropbox / "VMs" / "HDF5"
            dest_vm_dir.mkdir(parents=True, exist_ok=True)
            print(f"Move VMs/HDF5 file for fault {args.fault}...")
            shutil.move(
                tar_original_setup_files_from_dropbox
                / "VMs"
                / "HDF5"
                / f"{args.fault}_velocity_model.h5",
                dest_vm_dir / f"{args.fault}_velocity_model.h5",
            )
        else:
            raise ValueError(f"Unsupported version: {args.version}")

    print("Done processing all files!")


if __name__ == "__main__":
    main()
