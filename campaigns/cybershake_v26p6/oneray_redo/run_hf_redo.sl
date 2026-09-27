#!/bin/bash
#SBATCH --account=nesi00213
#SBATCH --job-name=hf_oneray_redo
#SBATCH --partition=genoa,milan
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=48
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --output=/home/arr65/v26p6_oneray_redo/hf/logs/hf_%A_%a.out
#SBATCH --error=/home/arr65/v26p6_oneray_redo/hf/logs/hf_%A_%a.err
#
# OneRay HF redo for PalliserKai and WellTeast (Cybershake v26p6), one
# realisation per array task: line TASK_ID+1 of CMDS and TARGETS, i.e. the
# PalliserKai median, REL01-37, then the WellTeast median, REL01-32 (0-70).
#
# Each command reproduces the realisation's original run except for the 1D
# model: Cant1D_v3-midQ_OneRay.1d rather than the leer default the originals
# fell back to (../hf_1d_model_fallback.md). make_hf_commands.py built the
# commands and preflight_hf.py checked them; this refuses to run if either
# file has changed.
#
# The original must first be moved to HF.leer (move_aside.py). This refuses to
# start while HF/Acc/HF.bin names any model other than OneRay. Afterwards it
# checks the new HF.bin against HF.leer/Acc/HF.bin: the same size, and a
# header identical but for the model name (every setting, the seed, the stoch
# file, the station table, and each station's e_dist and Vs).
#
# Sizing. Sung's logs give 14 s per station for PalliserKai (17760 stations)
# and 7-8 s for WellTeast (18432), so 48 tasks take about 1.4 h and 0.8 h;
# 4 h leaves room for slower nodes. A task whose HF.log says "Simulation
# completed" and whose HF.bin names OneRay only re-runs the checks. hf_sim.py
# resumes from its checkpoints, so a task that runs out of time is simply
# resubmitted. DRY_RUN=1 runs the guards and stops before hf_sim.py.

set -euo pipefail

DIR=/home/arr65/v26p6_oneray_redo
CMDS=$DIR/hf_commands.txt
CMDS_MD5=e87500b62b19d86b846eae0ca468a6b0
TARGETS=$DIR/targets.txt
TARGETS_MD5=53cec4ad8dda9a30d4ba530db60bdfe7
MODEL_NAME=Cant1D_v3-midQ_OneRay.1d

source /nesi/project/nesi00213/Environments/mrd87_4/py311/bin/activate

for pair in "$CMDS:$CMDS_MD5" "$TARGETS:$TARGETS_MD5"; do
    if [[ "$(md5sum < "${pair%%:*}" | cut -c1-32)" != "${pair##*:}" ]]; then
        echo "${pair%%:*} has changed since it was checked - refusing to run"
        exit 1
    fi
done
CMD=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$CMDS")
R=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$TARGETS")
if [[ -z "$CMD" || -z "$R" ]]; then
    echo "no command or target on line $((SLURM_ARRAY_TASK_ID + 1))"
    exit 1
fi
OUT=$R/HF/Acc/HF.bin
LOG=$R/HF/Acc/HF.log
ORIG=$R/HF.leer/Acc/HF.bin
if [[ "$(python -c 'import shlex, sys; print(shlex.split(sys.argv[1])[6])' "$CMD")" != "$OUT" ]]; then
    echo "line $((SLURM_ARRAY_TASK_ID + 1)) of $CMDS does not write $OUT - refusing to run"
    exit 1
fi
model_of() {
    python -c 'import sys; f = open(sys.argv[1], "rb"); f.seek(224); print(f.read(64).split(b"\0")[0].decode())' "$1"
}

echo "========================================="
echo "job          : ${SLURM_ARRAY_JOB_ID:-none}_${SLURM_ARRAY_TASK_ID} (${SLURM_JOB_ID:-none}) on $(hostname), ${SLURM_NTASKS:-0} tasks"
echo "realisation  : $R"
echo "commands     : $CMDS (md5 $CMDS_MD5), line $((SLURM_ARRAY_TASK_ID + 1))"
echo "python       : $(which python)"
echo "started      : $(date '+%Y-%m-%d %H:%M:%S')"
echo "command      : $CMD"
echo "========================================="

if [[ ! -f "$ORIG" ]]; then
    echo "$ORIG is missing: move the original aside first (move_aside.py) - refusing to run"
    exit 1
fi
done_already=0
if [[ -f "$OUT" ]]; then
    model=$(model_of "$OUT")
    if [[ "$model" != "$MODEL_NAME" ]]; then
        echo "$OUT names 1D model '$model', not $MODEL_NAME - refusing to touch it"
        exit 1
    fi
    if [[ -f "$LOG" ]] && grep -q "Simulation completed" "$LOG"; then
        echo "$LOG already says the simulation completed - only re-running the checks."
        done_already=1
    fi
fi
if [[ "${DRY_RUN:-0}" == 1 ]]; then
    echo "DRY_RUN: guards passed (output $([[ -f "$OUT" ]] && echo "present, OneRay" || echo absent)) - stopping here"
    exit 0
fi
if [[ "$done_already" == 0 ]]; then
    mkdir -p "$(dirname "$OUT")"
    eval "$CMD"
    echo "finished     : $(date '+%Y-%m-%d %H:%M:%S')"
fi

problems=0
if ! grep -q "Simulation completed" "$LOG"; then
    echo "CHECK FAILED: $LOG does not say the simulation completed"
    problems=1
fi
# Same size, and header + station table (0x200 + nstat x 0x18 bytes) identical
# to the original's but for the 1D model name at bytes 224-288.
if ! python - "$OUT" "$ORIG" "$MODEL_NAME" <<'EOF'
import os
import sys

import numpy as np

new, orig, model = sys.argv[1:]
nstat = int(np.fromfile(orig, "i4", 1)[0])
n = 0x200 + nstat * 0x18
with open(new, "rb") as f:
    a = f.read(n)
with open(orig, "rb") as f:
    b = f.read(n)
problems = []
if os.path.getsize(new) != os.path.getsize(orig):
    problems.append(f"size {os.path.getsize(new)}, original {os.path.getsize(orig)}")
new_model = a[224:288].split(b"\0")[0].decode()
if new_model != model:
    problems.append(f"model {new_model!r}")
if a[:224] != b[:224] or a[288:] != b[288:]:
    diff = [i for i in range(min(len(a), len(b))) if a[i] != b[i] and not 224 <= i < 288]
    problems.append(f"header differs from the original outside the model name at {len(diff)} bytes, first {diff[:5]}")
for p in problems:
    print(f"CHECK FAILED: {p}")
print(f"HF.bin       : {os.path.getsize(new)} bytes, model {model}, header "
      + ("identical to the original but for the model" if not problems else "NOT as expected"))
sys.exit(1 if problems else 0)
EOF
then
    problems=1
fi
exit $problems
