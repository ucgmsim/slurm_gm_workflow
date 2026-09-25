#!/bin/bash
#SBATCH --account=nesi00213
#SBATCH --job-name=bb_WellTeast
#SBATCH --partition=genoa,milan
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=16
#SBATCH --time=03:00:00
#SBATCH --mem=84G
#SBATCH --output=/home/arr65/bb_v26p6/logs/bb_%A_%a.out
#SBATCH --error=/home/arr65/bb_v26p6/logs/bb_%A_%a.err
#
# WellTeast BB against the canonical 18431-station list, one realisation per
# array task (line TASK_ID+1 of TARGET_LIST), run from the pinned branch
# nesi-cybershake-v26p6.
#
# bb_sim arguments, VM directory and vs30 file are those of Sung's six
# completed WellTeast runs (BB/bb_sim_command.sh, 2026-07-07). Differences:
#   - code: bb_sim.py reconciles LF and HF station sets by name;
#   - --station-list: drops 320077e from every realisation (user decision,
#     docs/superpowers/specs/2026-09-21-bb-station-reconciliation-design.md);
#   - 16 MPI tasks rather than 4. Each station is computed independently
#     and written at its own offset, so the output does not depend on this.
# Like the PalliserKai REL08 run, it omits the side effects of Sung's
# wrapper: fix_old_nesi_path.sh and the status-DB update (done by hand after
# verification).
#
# Never overwrites a BB.bin. It refuses if one exists, unless RESUME=1 and
# the file is exactly the canonical size, i.e. an interrupted run of this same
# job that bb_sim can resume from its own checkpoints. The six original
# 18432-station outputs were moved to BB.bin.with_320077e before submission.

set -euo pipefail

E=/nesi/project/nesi00213/Environments/arr65_v26p6/workflow
LIST=/nesi/project/nesi00213/Environments/arr65_v26p6/wellteast_bb_stations_18431.txt
V=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Data/VMs/WellTeast
VS30=/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.vs30
# 0x500 header + 18431 stations x (44-byte record + 59848 samples x 3 comps x 4 bytes)
CANONICAL_SIZE=13237514100

R=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$TARGET_LIST")
if [[ -z "$R" ]]; then
    echo "no target on line $((SLURM_ARRAY_TASK_ID + 1)) of $TARGET_LIST"
    exit 1
fi
OUT=$R/BB/Acc/BB.bin

source /nesi/project/nesi00213/Environments/mrd87_4/py311/bin/activate
# Without this the venv resolves `workflow` to mrd87_4's old checkout,
# which has no bb_station_set.
export PYTHONPATH=$E

echo "========================================="
echo "job          : ${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID} (${SLURM_JOB_ID}) on $(hostname)"
echo "realisation  : $R"
echo "code         : $E"
echo "commit       : $(git -C $E rev-parse HEAD)"
echo "branch       : $(git -C $E rev-parse --abbrev-ref HEAD)"
echo "tree clean   : $([ -z "$(git -C $E status --porcelain)" ] && echo yes || echo NO)"
echo "python       : $(which python)"
echo "workflow     : $(python -c 'import workflow; print(workflow.__file__)')"
echo "station list : $LIST (md5 $(md5sum < $LIST | cut -c1-32))"
echo "started      : $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================="

if [[ -e "$OUT" ]]; then
    size=$(stat -c %s "$OUT")
    if [[ "${RESUME:-0}" == 1 && "$size" == "$CANONICAL_SIZE" ]]; then
        echo "RESUME=1 and $OUT is the canonical size: letting bb_sim resume from its checkpoints."
    else
        echo "$OUT already exists ($size bytes) - refusing to overwrite."
        exit 1
    fi
fi
mkdir -p "$R/BB/Acc"

srun python "$E/workflow/calculation/bb_sim.py" \
    "$R/LF/OutBin" \
    "$V" \
    "$R/HF/Acc/HF.bin" \
    "$VS30" \
    "$OUT" \
    --flo 1.0 --fmin 0.5 --fmidbot 1.0 --dt 0.005 --no-lf-amp \
    --station-list "$LIST"

echo "finished     : $(date '+%Y-%m-%d %H:%M:%S')"
size=$(stat -c %s "$OUT")
echo "BB.bin size  : $size ($([[ "$size" == "$CANONICAL_SIZE" ]] && echo canonical || echo UNEXPECTED))"
