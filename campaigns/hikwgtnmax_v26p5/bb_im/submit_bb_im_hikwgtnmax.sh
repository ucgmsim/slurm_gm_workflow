#!/bin/bash
# Submit HikWgtnmax BB (array 0-50) and IM, once the LF NetCDF reader's commit is
# deployed at E.
#
# IM runs Sung's run_im_job_array.sl with the sarah2024 environment, as for
# PalliserKai and WellTeast. Its command for these realisations was checked to
# match theirs apart from paths. It runs as 51 separate single-task jobs, each
# waiting for its own BB task (afterok). NeSI cancels any job whose dependency can
# never be met (kill_invalid_depend), so one IM array chained to the BB array
# would let a single failed BB task cancel every pending IM, as happened on
# 2026-09-28. Sung's wrapper also updates his status DB, which has no HikWgtnmax
# rows, so that update does nothing.
#
# Refuses unless the pre-flight passes and every BB task passes its guards in a
# dry run. Logs the submissions in ../submissions.txt.
# Usage: submit_bb_im_hikwgtnmax.sh (on a NeSI login node)

set -euo pipefail

DIR=/home/arr65/hikwgtnmax_v26p5/bb_im
E=/nesi/project/nesi00213/Environments/arr65_v26p6_netcdf/workflow
IM_WRAPPER=/nesi/nobackup/nesi00213/RunFolder/submit/run_im_job_array.sl
SARAH=/nesi/project/nesi00213/Environments/sarah2024

cd "$DIR"
mkdir -p im/logs
source /nesi/project/nesi00213/Environments/mrd87_4/py311/bin/activate
if ! PYTHONPATH=$E python preflight_bb_hikwgtnmax.py targets.txt > preflight_submit.log 2>&1; then
    echo "pre-flight failed - see $DIR/preflight_submit.log"
    exit 1
fi
for i in $(seq 0 50); do
    if ! SLURM_ARRAY_TASK_ID=$i DRY_RUN=1 bash run_bb_hikwgtnmax.sl > /dev/null 2>&1; then
        echo "BB task $i fails its guards: run 'SLURM_ARRAY_TASK_ID=$i DRY_RUN=1 bash run_bb_hikwgtnmax.sl' to see why"
        exit 1
    fi
done
echo "pre-flight and BB dry runs passed"

BB=$(sbatch --parsable --array=0-50 run_bb_hikwgtnmax.sl)
IMS=()
for i in $(seq 0 50); do
    IMS+=("$(cd im && sbatch --parsable --account=nesi00213 --partition=genoa,milan --ntasks-per-node=32 \
        --mem=84G --time=08:00:00 --array="$i" --dependency="afterok:${BB}_$i" --job-name=im_HikWgtnmax \
        --export=ALL,TARGET_LIST=$DIR/targets.txt,gmsim=$SARAH "$IM_WRAPPER")")
done
now=$(date '+%Y-%m-%d %H:%M:%S')
{
    echo "$now job $BB: HikWgtnmax BB array 0-50 (run_bb_hikwgtnmax.sl, LF NetCDF reader at $(git -C $E rev-parse --short HEAD), 16 tasks/84G/6h)"
    echo "$now jobs ${IMS[0]}..${IMS[50]}: HikWgtnmax IM, one single-task job per realisation, each afterok its own BB task (Sung's run_im_job_array.sl, sarah2024, 32 tasks/84G/8h)"
} >> ../submissions.txt
tail -n 2 ../submissions.txt
