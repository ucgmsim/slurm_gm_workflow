#!/bin/bash
#SBATCH --account=nesi00213
#SBATCH --job-name=bb_oneray_redo
#SBATCH --partition=genoa,milan
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=16
#SBATCH --time=03:00:00
#SBATCH --mem=84G
#SBATCH --output=/home/arr65/v26p6_oneray_redo/bb/logs/bb_%A_%a.out
#SBATCH --error=/home/arr65/v26p6_oneray_redo/bb/logs/bb_%A_%a.err
#
# BB for the OneRay HF redo (PalliserKai and WellTeast, Cybershake v26p6), one
# realisation per array task: line TASK_ID+1 of TARGETS, the same indices as
# the HF array. Submit with --array=0-41,43-70 (42 is WellTeast_REL04, which
# has no LF) and --dependency=aftercorr:<HF array>.
#
# Code, arguments and station sets are those of the leer BB runs it replaces
# (run_bb_wellteast.sl, job 9310308, and PalliserKai REL08, job 9216501):
#   - the pinned branch at 2c6a79a8, whose bb_sim.py reconciles LF and HF
#     station sets by name;
#   - Sung's arguments: --flo 1.0 --fmin 0.5 --fmidbot 1.0 --dt 0.005
#     --no-lf-amp, the fault's v26p6 VM directory and the same vs30 file;
#   - WellTeast with the canonical 18431-station list, PalliserKai with none
#     (its LF and HF cover the same 17760 stations once duplicates collapse).
# Only the HF input differs. 16 tasks: each station is computed independently
# and written at its own offset, so the output does not depend on the count.
#
# Guards. The HF must be the redo's: its header names OneRay, its HF.log says
# the simulation completed, and it is the size of HF.leer/Acc/HF.bin. Never
# overwrites a BB.bin unless RESUME=1 and it is the original's size, i.e. an
# interrupted run of this job that bb_sim resumes from its own checkpoints.
# DRY_RUN=1 runs the guards and stops before bb_sim.py.
#
# Afterwards the new BB.bin must be the size of BB.leer/Acc/BB.bin, with an
# identical header and station table (0x500 + nstat x 44 bytes): the same
# stations in the same order, and the same e_dist, HF and LF reference Vs,
# and site vs30 in every record. Only the waveforms may differ.

set -euo pipefail

DIR=/home/arr65/v26p6_oneray_redo
TARGETS=$DIR/targets.txt
TARGETS_MD5=53cec4ad8dda9a30d4ba530db60bdfe7
E=/nesi/project/nesi00213/Environments/arr65_v26p6/workflow
COMMIT=2c6a79a8
DATA=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Data
VS30=/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.vs30
WELLTEAST_LIST=/nesi/project/nesi00213/Environments/arr65_v26p6/wellteast_bb_stations_18431.txt
WELLTEAST_LIST_MD5=b4a51ad3b0380950c5db6d569a3ed23a
MODEL_NAME=Cant1D_v3-midQ_OneRay.1d

if [[ "$(md5sum < "$TARGETS" | cut -c1-32)" != "$TARGETS_MD5" ]]; then
    echo "$TARGETS has changed since it was checked - refusing to run"
    exit 1
fi
R=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$TARGETS")
if [[ -z "$R" ]]; then
    echo "no target on line $((SLURM_ARRAY_TASK_ID + 1)) of $TARGETS"
    exit 1
fi
FAULT=$(basename "$(dirname "$R")")
case "$FAULT" in
    PalliserKai) LIST_ARGS=() ;;
    WellTeast)
        if [[ "$(md5sum < "$WELLTEAST_LIST" | cut -c1-32)" != "$WELLTEAST_LIST_MD5" ]]; then
            echo "$WELLTEAST_LIST has changed - refusing to run"
            exit 1
        fi
        LIST_ARGS=(--station-list "$WELLTEAST_LIST") ;;
    *) echo "unexpected fault $FAULT"; exit 1 ;;
esac
V=$DATA/VMs/$FAULT
HF=$R/HF/Acc/HF.bin
OUT=$R/BB/Acc/BB.bin
ORIG=$R/BB.leer/Acc/BB.bin

source /nesi/project/nesi00213/Environments/mrd87_4/py311/bin/activate
# Without this the venv resolves `workflow` to mrd87_4's old checkout,
# which has no bb_station_set.
export PYTHONPATH=$E

echo "========================================="
echo "job          : ${SLURM_ARRAY_JOB_ID:-none}_${SLURM_ARRAY_TASK_ID} (${SLURM_JOB_ID:-none}) on $(hostname)"
echo "realisation  : $R"
echo "code         : $E"
echo "commit       : $(git -C $E rev-parse HEAD)"
echo "tree clean   : $([ -z "$(git -C $E status --porcelain)" ] && echo yes || echo NO)"
echo "python       : $(which python)"
echo "workflow     : $(python -c 'import workflow; print(workflow.__file__)')"
echo "station list : ${LIST_ARGS[*]:-(none: LF and HF sets reconciled by name)}"
echo "started      : $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================="

if [[ "$(git -C $E rev-parse HEAD)" != "$COMMIT"* || -n "$(git -C $E status --porcelain)" ]]; then
    echo "$E is not a clean checkout of $COMMIT - refusing to run"
    exit 1
fi
if [[ ! -d "$R/LF/OutBin" ]]; then
    echo "$R has no LF/OutBin - refusing to run"
    exit 1
fi
if [[ ! -f "$ORIG" ]]; then
    echo "$ORIG is missing: move the original aside first (move_aside.py) - refusing to run"
    exit 1
fi
hf_model=$(python -c 'import sys; f = open(sys.argv[1], "rb"); f.seek(224); print(f.read(64).split(b"\0")[0].decode())' "$HF" 2>/dev/null || echo missing)
if [[ "$hf_model" != "$MODEL_NAME" ]] || ! grep -q "Simulation completed" "$R/HF/Acc/HF.log" 2>/dev/null \
        || [[ "$(stat -c %s "$HF")" != "$(stat -c %s "$R/HF.leer/Acc/HF.bin")" ]]; then
    echo "$HF is not a completed OneRay redo (model: $hf_model) - refusing to run"
    exit 1
fi
if [[ -e "$OUT" ]]; then
    size=$(stat -c %s "$OUT")
    if [[ "${RESUME:-0}" == 1 && "$size" == "$(stat -c %s "$ORIG")" ]]; then
        echo "RESUME=1 and $OUT is the original's size: letting bb_sim resume from its checkpoints."
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
    "$R/LF/OutBin" \
    "$V" \
    "$HF" \
    "$VS30" \
    "$OUT" \
    --flo 1.0 --fmin 0.5 --fmidbot 1.0 --dt 0.005 --no-lf-amp \
    "${LIST_ARGS[@]}"

echo "finished     : $(date '+%Y-%m-%d %H:%M:%S')"
python - "$OUT" "$ORIG" <<'EOF'
import os
import sys

import numpy as np

new, orig = sys.argv[1:]
nstat = int(np.fromfile(orig, "i4", 1)[0])
n = 0x500 + nstat * 44
with open(new, "rb") as f:
    a = f.read(n)
with open(orig, "rb") as f:
    b = f.read(n)
problems = []
if os.path.getsize(new) != os.path.getsize(orig):
    problems.append(f"size {os.path.getsize(new)}, original {os.path.getsize(orig)}")
if a != b:
    diff = [i for i in range(min(len(a), len(b))) if a[i] != b[i]]
    problems.append(f"header differs from the original at {len(diff)} bytes, first {diff[:5]}")
for p in problems:
    print(f"CHECK FAILED: {p}")
print(f"BB.bin       : {os.path.getsize(new)} bytes, {nstat} stations, header "
      + ("identical to the original" if not problems else "NOT as expected"))
sys.exit(1 if problems else 0)
EOF
