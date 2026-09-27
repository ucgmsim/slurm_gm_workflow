#!/bin/bash
# Submit the OneRay redo for PalliserKai and WellTeast, after move_aside.py --apply:
#   HF  array 0-70, one realisation per task (run_hf_redo.sl);
#   BB  array 0-41,43-70, each task waiting for its HF task (aftercorr);
#   IM  array 0-41,43-70, each task waiting for its BB task, with Sung's wrapper
#       run_im_job_array.sl and the sarah2024 environment, as on 2026-09-25.
# Index 42 is WellTeast_REL04, which has HF but no LF, so no BB or IM.
#
# Refuses unless preflight_hf.py passes with every target moved aside and every
# HF task passes its guards in a dry run. Each submission is logged in
# submissions.txt. Usage: submit_redo.sh (on a NeSI login node)

set -euo pipefail

DIR=/home/arr65/v26p6_oneray_redo
IM_WRAPPER=/nesi/nobackup/nesi00213/RunFolder/submit/run_im_job_array.sl
SARAH=/nesi/project/nesi00213/Environments/sarah2024
BB_IDX=0-41,43-70

cd "$DIR"
source /nesi/project/nesi00213/Environments/mrd87_4/py311/bin/activate
if ! python preflight_hf.py targets.txt hf_commands.txt --expect moved > preflight_submit.log 2>&1; then
    echo "pre-flight failed - see $DIR/preflight_submit.log"
    exit 1
fi
for i in $(seq 0 70); do
    if ! SLURM_ARRAY_TASK_ID=$i DRY_RUN=1 bash run_hf_redo.sl > /dev/null 2>&1; then
        echo "HF task $i fails its guards: run 'SLURM_ARRAY_TASK_ID=$i DRY_RUN=1 bash run_hf_redo.sl' to see why"
        exit 1
    fi
done
echo "pre-flight and HF dry runs passed"

HF_JOB=$(sbatch --parsable --array=0-70 run_hf_redo.sl)
BB_JOB=$(sbatch --parsable --array=$BB_IDX --dependency=aftercorr:$HF_JOB run_bb_redo.sl)
IM_JOB=$(cd im && sbatch --parsable --account=nesi00213 --partition=genoa,milan --ntasks-per-node=32 \
    --mem=84G --time=04:00:00 --array=$BB_IDX --dependency=aftercorr:$BB_JOB --job-name=im_oneray_redo \
    --export=ALL,TARGET_LIST=$DIR/targets.txt,gmsim=$SARAH "$IM_WRAPPER")
now=$(date '+%Y-%m-%d %H:%M:%S')
{
    echo "$now job $HF_JOB: HF redo array 0-70 (run_hf_redo.sl, OneRay, 48 tasks/32G/4h, nesi00213, genoa,milan)"
    echo "$now job $BB_JOB: BB redo array $BB_IDX (run_bb_redo.sl, aftercorr:$HF_JOB, 16 tasks/84G/3h)"
    echo "$now job $IM_JOB: IM redo array $BB_IDX (Sung's run_im_job_array.sl, sarah2024, aftercorr:$BB_JOB, 32 tasks/84G/4h)"
} >> submissions.txt
tail -n 3 submissions.txt
