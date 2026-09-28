#!/bin/bash
#SBATCH --account=nesi00213
#SBATCH --job-name=lf_to_netcdf
#SBATCH --partition=genoa,milan
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=160G
#SBATCH --time=03:00:00
#SBATCH --output=/home/arr65/hikwgtnmax_v26p5/netcdf_reader_validation/logs/convert_%j.out
#SBATCH --error=/home/arr65/hikwgtnmax_v26p5/netcdf_reader_validation/logs/convert_%j.err
#
# Convert an EMOD3D OutBin to the new workflow's LF NetCDF exactly as the
# Cybershake v26p5 HikWgtnmax LF was converted on Cascade:
# workflow `pegasus` 7e465c5's lf-to-xarray is
#     timeseries.read_lfseis_directory(outbin).to_netcdf(out, engine="h5netcdf")
# with qcore-utils 2025.12.2. The venv pins that and the other versions in
# 7e465c5's uv.lock (numpy 2.3.5, xarray 2026.2.0, h5netcdf 1.8.1, h5py 3.15.1,
# pandas 2.3.3, scipy 1.17.0). Cascade ran Python 3.13 and this venv runs 3.11,
# which may change float32 results in the last bit. That doesn't matter for
# this job's purpose: testing the reader that inverts this conversion.
#
# Reads the OutBin and writes only OUT, which must not exist yet. It holds the
# whole record in memory (3 components x stations x nt float32, several copies).
# Usage: sbatch convert_lf_to_netcdf.sl OUTBIN OUT

set -euo pipefail

OUTBIN=${1:?OutBin directory}
OUT=${2:?output NetCDF}
VENV=/home/arr65/venvs/qcore2025122

if [[ -e "$OUT" ]]; then
    echo "$OUT already exists - refusing to overwrite it"
    exit 1
fi
echo "job     : ${SLURM_JOB_ID} on $(hostname)"
echo "outbin  : $OUTBIN ($(ls "$OUTBIN" | grep -c 'seis-.*\.e3d') seis files)"
echo "out     : $OUT"
echo "started : $(date '+%F %T')"
"$VENV/bin/python" - "$OUTBIN" "$OUT" <<'EOF'
import sys

import h5netcdf, h5py, numpy, pandas, scipy, xarray
from importlib.metadata import version
from qcore import timeseries

print("versions:", {m: version(m) for m in ("numpy", "xarray", "h5netcdf", "h5py", "pandas", "scipy", "qcore-utils")})
outbin, out = sys.argv[1:]
timeseries.read_lfseis_directory(outbin).to_netcdf(out, engine="h5netcdf")
EOF
echo "finished: $(date '+%F %T'), $(stat -c %s "$OUT") bytes"
