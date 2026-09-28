#!/bin/bash
#SBATCH --account=nesi00213
#SBATCH --job-name=bb_HikWgtnmax
#SBATCH --partition=genoa,milan
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=16
#SBATCH --time=06:00:00
#SBATCH --mem=84G
#SBATCH --output=/home/arr65/hikwgtnmax_v26p5/bb_im/logs/bb_%A_%a.out
#SBATCH --error=/home/arr65/hikwgtnmax_v26p5/bb_im/logs/bb_%A_%a.err
#
# HikWgtnmax BB (Cybershake v26p5), one realisation per array task: line
# TASK_ID+1 of TARGETS, i.e. the median, then REL01-REL50 (array 0-50).
#
# Inputs:
#   - LF: the NetCDF computed on Cascade, read by bb_sim's LF NetCDF reader
#     (workflow/calculation/lf_netcdf.py) with the realisation's staged e3d.par
#     and the fault's station coordinates (make_bb_inputs.py).
#   - HF: the OneRay run of 2026-09-26/27.
#   - VM: v26p5/Data/VMs/HikWgtnmax, whose vs3dfile.s came from Dropbox on
#     2026-09-28.
#   - Arguments: those of v26p5's root_params (bb dt 0.005, fmin 0.5, fmidbot
#     1.0, no LF amplification; flo 1.0) and the vs30 file it names, the same
#     as for the PalliserKai and WellTeast BB.
#
# Guards:
#   - the code is a clean checkout of COMMIT;
#   - HF is the finished OneRay run, and the NetCDF and e3d.par are there;
#   - never overwrites a BB.bin unless RESUME=1 and it is the expected size.
#     bb_sim then resumes from its checkpoints, which this code zero-initialises
#     (the 2c6a79a8 code did not, which corrupted the 2026-09-28 resumes).
# Afterwards check_bb_output.py checks every station record against its source
# and every waveform row. DRY_RUN=1 stops before bb_sim.

set -euo pipefail

DIR=/home/arr65/hikwgtnmax_v26p5/bb_im
TARGETS=$DIR/targets.txt
TARGETS_MD5=7f953932d21f00684358075c5cbec862
E=/nesi/project/nesi00213/Environments/arr65_v26p6_netcdf/workflow
COMMIT=e4ef522544d7a71d5b13d20022a3bd62dba286ee
STAGE=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Runs/HikWgtnmax_LF_from_Cascade
STATCORDS=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Runs/HikWgtnmax/fd_rt01-h0.100.statcords
V=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Data/VMs/HikWgtnmax
VS30=/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.vs30
HF_MODEL=Cant1D_v3-midQ_OneRay.1d
HF_SIZE=14643091208
# 0x500 header + 17186 stations x (44-byte record + 71601 samples x 3 comps x 4 bytes)
BB_SIZE=14767174896

if [[ "$(md5sum < "$TARGETS" | cut -c1-32)" != "$TARGETS_MD5" ]]; then
    echo "$TARGETS has changed since it was checked - refusing to run"
    exit 1
fi
R=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$TARGETS")
[[ -n "$R" ]] || { echo "no target on line $((SLURM_ARRAY_TASK_ID + 1)) of $TARGETS"; exit 1; }
NAME=$(basename "$R")
NC=$STAGE/${NAME}_seis.nc
E3D=$STAGE/e3d_par/${NAME}_e3d.par
HF=$R/HF/Acc/HF.bin
OUT=$R/BB/Acc/BB.bin

source /nesi/project/nesi00213/Environments/mrd87_4/py311/bin/activate
# Without this the venv resolves `workflow` to mrd87_4's old checkout.
export PYTHONPATH=$E

echo "========================================="
echo "job          : ${SLURM_ARRAY_JOB_ID:-none}_${SLURM_ARRAY_TASK_ID} (${SLURM_JOB_ID:-none}) on $(hostname)"
echo "realisation  : $R"
echo "code         : $E at $(git -C $E rev-parse HEAD), clean: $([ -z "$(git -C $E status --porcelain)" ] && echo yes || echo NO)"
echo "workflow     : $(python -c 'import workflow; print(workflow.__file__)')"
echo "LF           : $NC with $E3D"
echo "started      : $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================="

if [[ "$(git -C $E rev-parse HEAD)" != "$COMMIT"* || -n "$(git -C $E status --porcelain)" ]]; then
    echo "$E is not a clean checkout of $COMMIT - refusing to run"
    exit 1
fi
for f in "$NC" "$E3D" "$HF"; do
    [[ -f "$f" ]] || { echo "$f is missing - refusing to run"; exit 1; }
done
hf_model=$(python -c 'import sys; f = open(sys.argv[1], "rb"); f.seek(224); print(f.read(64).split(b"\0")[0].decode())' "$HF")
if [[ "$hf_model" != "$HF_MODEL" || "$(stat -c %s "$HF")" != "$HF_SIZE" ]] || ! grep -q "Simulation completed" "$R/HF/Acc/HF.log"; then
    echo "$HF is not the finished OneRay run (model $hf_model) - refusing to run"
    exit 1
fi
if [[ -e "$OUT" ]]; then
    size=$(stat -c %s "$OUT")
    if [[ "${RESUME:-0}" == 1 && "$size" == "$BB_SIZE" ]]; then
        echo "RESUME=1 and $OUT is the expected size: letting bb_sim resume from its checkpoints."
    else
        echo "$OUT already exists ($size bytes) - refusing to overwrite."
        exit 1
    fi
fi
if [[ "${DRY_RUN:-0}" == 1 ]]; then
    echo "DRY_RUN: guards passed - stopping here"
    exit 0
fi
mkdir -p "$R/BB/Acc"

srun python "$E/workflow/calculation/bb_sim.py" \
    "$NC" "$V" "$HF" "$VS30" "$OUT" \
    --flo 1.0 --fmin 0.5 --fmidbot 1.0 --dt 0.005 --no-lf-amp \
    --lf-e3d-par "$E3D" --lf-statcords "$STATCORDS"

echo "finished     : $(date '+%Y-%m-%d %H:%M:%S')"
python "$DIR/check_bb_output.py" "$OUT" "$HF" "$NC" "$STATCORDS" "$V" "$VS30"
