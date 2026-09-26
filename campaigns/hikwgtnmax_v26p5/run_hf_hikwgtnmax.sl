#!/bin/bash
#SBATCH --account=nesi00213
#SBATCH --job-name=hf_HikWgtnmax
#SBATCH --partition=genoa,milan
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=96
#SBATCH --mem=64G
#SBATCH --time=16:00:00
#SBATCH --output=/home/arr65/hikwgtnmax_v26p5/hf/logs/hf_%A_%a.out
#SBATCH --error=/home/arr65/hikwgtnmax_v26p5/hf/logs/hf_%A_%a.err
#
# HikWgtnmax HF (Cybershake v26p5), one realisation per array task: line
# TASK_ID+1 of CMDS, i.e. the median, then REL01-REL50 (array 0-50).
#
# Each line is the command Sung's generator produces for the realisation,
# with the 1D model and HF binary named explicitly (make_hf_commands.sh):
# Cant1D_v3-midQ_OneRay.1d, which every Cybershake config names, rather than
# hf_sim.py's leer default that the v26p6 runs fell back to. The commands were
# checked by preflight_hf.py; this refuses to run if the file has changed.
#
# Differs from Sung's run_hf_job_array.sl: it does not run
# fix_old_nesi_path.sh (that would repoint these v26p5 configs at v26p6) and
# does not update any status DB.
#
# Sizing. A timing test on 2026-09-26 (REL01, station HNPS, one process) took
# 184 s for one station, with 154 MB max RSS; the source has 8064 subfaults
# and 71001 time steps. So 17186 stations over 96 tasks is about 9 h, and 16 h
# leaves room for slower nodes. Each task holds a physical core, i.e. 2 CPUs,
# so 28 of these jobs fit under the 5376-CPU per-user QOS limit, and all 51
# run in two waves.
#
# A task whose HF.log already says "Simulation completed" is skipped.
# hf_sim.py checkpoints per station and resumes when rerun with the same
# command, so a task that runs out of time is simply resubmitted.

set -euo pipefail

CMDS=/home/arr65/hikwgtnmax_v26p5/hf/hf_commands.txt
CMDS_MD5=6a9811d302ca58627caa2f7ab4d0525b
MODEL_NAME=Cant1D_v3-midQ_OneRay.1d
# 0x200 header + 17186 stations x (0x18 record + 71001 samples x 3 comps x 4 bytes)
EXPECTED_SIZE=14643091208

source /nesi/project/nesi00213/Environments/mrd87_4/py311/bin/activate

if [[ "$(md5sum < "$CMDS" | cut -c1-32)" != "$CMDS_MD5" ]]; then
    echo "$CMDS has changed since it was checked - refusing to run"
    exit 1
fi
CMD=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$CMDS")
if [[ -z "$CMD" ]]; then
    echo "no command on line $((SLURM_ARRAY_TASK_ID + 1)) of $CMDS"
    exit 1
fi
OUT=$(python -c 'import shlex, sys; print(shlex.split(sys.argv[1])[6])' "$CMD")
LOG=$(dirname "$OUT")/HF.log

echo "========================================="
echo "job          : ${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID} (${SLURM_JOB_ID}) on $(hostname), ${SLURM_NTASKS} tasks"
echo "output       : $OUT"
echo "commands     : $CMDS (md5 $CMDS_MD5), line $((SLURM_ARRAY_TASK_ID + 1))"
echo "python       : $(which python)"
echo "started      : $(date '+%Y-%m-%d %H:%M:%S')"
echo "command      : $CMD"
echo "========================================="

if [[ -f "$LOG" ]] && grep -q "Simulation completed" "$LOG"; then
    echo "$LOG already says the simulation completed - nothing to do."
    exit 0
fi
mkdir -p "$(dirname "$OUT")"

eval "$CMD"

echo "finished     : $(date '+%Y-%m-%d %H:%M:%S')"
problems=0
if ! grep -q "Simulation completed" "$LOG"; then
    echo "CHECK FAILED: $LOG does not say the simulation completed"
    problems=1
fi
size=$(stat -c %s "$OUT")
if [[ "$size" != "$EXPECTED_SIZE" ]]; then
    echo "CHECK FAILED: HF.bin is $size bytes, expected $EXPECTED_SIZE"
    problems=1
fi
# qcore HFSeis header: 16 int32, 24 float32, then the stoch file and 1D model names (64 bytes each)
model=$(python -c 'import sys; f = open(sys.argv[1], "rb"); f.seek(224); print(f.read(64).split(b"\0")[0].decode())' "$OUT")
if [[ "$model" != "$MODEL_NAME" ]]; then
    echo "CHECK FAILED: HF.bin header names 1D model '$model', expected $MODEL_NAME"
    problems=1
fi
echo "HF.bin       : $size bytes, 1D model $model"
exit $problems
