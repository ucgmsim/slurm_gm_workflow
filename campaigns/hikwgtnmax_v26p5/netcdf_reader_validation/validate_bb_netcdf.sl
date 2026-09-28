#!/bin/bash
#SBATCH --account=nesi00213
#SBATCH --job-name=bb_netcdf_validate
#SBATCH --partition=genoa,milan
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=16
#SBATCH --time=05:00:00
#SBATCH --mem=84G
#SBATCH --output=/home/arr65/hikwgtnmax_v26p5/netcdf_reader_validation/logs/validate_%j.out
#SBATCH --error=/home/arr65/hikwgtnmax_v26p5/netcdf_reader_validation/logs/validate_%j.err
#
# Validate bb_sim's LF NetCDF reader (workflow/calculation/lf_netcdf.py) on
# PalliserKai REL01, whose OutBin was converted with the same converter as the
# HikWgtnmax LF (convert_lf_to_netcdf.sl):
#   1. compare_readers.py: LFNetCDF on the NetCDF against LFSeis on the OutBin;
#   2. bb_sim from the NetCDF, with the same HF, VM, vs30 file and flags as the
#      production OneRay redo, written to the validation folder;
#   3. compare_bb_files.py: that BB.bin against the production BB.bin from the
#      OutBin, if that exists and passed its own check.
# Writes only under VAL. The code is a test copy of the pinned branch with the
# reader added, at CODE, never the production deployment.

set -euo pipefail

VAL=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Runs/HikWgtnmax_LF_from_Cascade/reader_validation
CODE=/nesi/project/nesi00213/Environments/arr65_v26p6_netcdf_test/workflow
R=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Runs/PalliserKai/PalliserKai_REL01
NC=$VAL/PalliserKai_REL01_seis.nc
STATCORDS=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Runs/PalliserKai/fd_rt01-h0.100.statcords
V=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Data/VMs/PalliserKai
VS30=/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.vs30
OUT=$VAL/BB/Acc/BB.bin
HERE=/home/arr65/hikwgtnmax_v26p5/netcdf_reader_validation

source /nesi/project/nesi00213/Environments/mrd87_4/py311/bin/activate
export PYTHONPATH=$CODE
echo "job      : ${SLURM_JOB_ID} on $(hostname)"
echo "code     : $CODE ($(git -C $CODE rev-parse --short HEAD) + $(git -C $CODE status --porcelain | wc -l) changed files)"
echo "lf_netcdf: md5 $(md5sum < $CODE/workflow/calculation/lf_netcdf.py | cut -c1-12), bb_sim md5 $(md5sum < $CODE/workflow/calculation/bb_sim.py | cut -c1-12)"
echo "started  : $(date '+%F %T')"

if [[ -e "$OUT" ]]; then
    echo "$OUT already exists - refusing to overwrite it"
    exit 1
fi

echo "=== 1. readers"
python "$HERE/compare_readers.py" "$R/LF/OutBin" "$NC" "$R/LF/e3d.par" "$STATCORDS" 1.0

echo "=== 2. bb_sim from the NetCDF ($(date '+%F %T'))"
mkdir -p "$VAL/BB/Acc"
srun python "$CODE/workflow/calculation/bb_sim.py" \
    "$NC" "$V" "$R/HF/Acc/HF.bin" "$VS30" "$OUT" \
    --flo 1.0 --fmin 0.5 --fmidbot 1.0 --dt 0.005 --no-lf-amp \
    --lf-e3d-par "$R/LF/e3d.par" --lf-statcords "$STATCORDS"
echo "finished : $(date '+%F %T')"

echo "=== 3. against the production BB from the OutBin"
if [[ -f "$R/BB/Acc/BB.bin" ]]; then
    python "$HERE/compare_bb_files.py" "$OUT" "$R/BB/Acc/BB.bin" 1e-5
else
    echo "production BB.bin not there yet - run compare_bb_files.py later"
fi
