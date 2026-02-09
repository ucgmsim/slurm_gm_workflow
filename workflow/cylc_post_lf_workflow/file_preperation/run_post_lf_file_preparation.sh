#!/bin/bash
#
# Script to run post-LF file preparation steps sequentially.
# Downloads large files from Dropbox, extracts them, and deploys them for post-LF stages.
#
# Usage: ./run_post_lf_file_preparation.sh <version> <fault>
# Example: ./run_post_lf_file_preparation.sh v25p11 WhiteCk

set -e  # Exit on error

# Activate the Python virtual environment (required for all Python scripts and tools)
source ~/venvs/velocity_modelling_venv/bin/activate

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check for required arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <version> <fault>"
    echo "Example: $0 v25p11 WhiteCk"
    exit 1
fi

VERSION="$1"
FAULT="$2"

echo "============================================================"
echo "Starting post-LF file preparation"
echo "Version: ${VERSION}"
echo "Fault: ${FAULT}"
echo "============================================================"
echo ""

# Step 1: Download large files from Dropbox
echo "[Step 1/4] Downloading large files from Dropbox..."
python3 "${SCRIPT_DIR}/download_large_files.py" "${VERSION}" "${FAULT}"
echo ""

# Step 2: Extract large files from Dropbox download
echo "[Step 2/4] Extracting large files from Dropbox download..."
python3 "${SCRIPT_DIR}/extract_large_files_from_dropbox_download.py" "${VERSION}" "${FAULT}"
echo ""

# Step 3: Deploy files for post-LF stages
echo "[Step 3/4] Deploying files for post-LF stages..."
python3 "${SCRIPT_DIR}/deploy_files_for_post_lf_stages.py" "${VERSION}" "${FAULT}"
echo ""

# Step 4: Convert HDF5 velocity model to EMOD3D format
echo "[Step 4/4] Converting HDF5 velocity model to EMOD3D format..."
hdf5-to-emod3d \
    "/scratch/projects/rch-quakecore/Cybershake/${VERSION}/Data/VMs/${FAULT}/${FAULT}_velocity_model.h5" \
    "/scratch/projects/rch-quakecore/Cybershake/${VERSION}/Data/VMs/${FAULT}"
echo ""

echo "============================================================"
echo "Post-LF file preparation completed successfully!"
echo "============================================================"