#!/bin/bash
# Generate the HikWgtnmax HF commands, one line per realisation (median first,
# then REL01-REL50), with Sung's generator run print-only. Then name the 1D
# velocity model and the HF binary explicitly on every command.
#
# Why explicit: v26p5's root_params gives the 1D model's KISTI path, which the
# generator silently drops on NeSI, so hf_sim.py would fall back to its leer
# default (see ../cybershake_v26p6/hf_1d_model_fallback.md). The binary is
# pinned because qcore picks it by hostname: compute nodes resolve it to the
# path below, the same binary as Sung's v26p6 HF runs, but a login* host is
# taken for KISTI.
#
# Writes only OUT and its generation log. Usage: make_hf_commands.sh [OUT]

set -euo pipefail

R=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Runs/HikWgtnmax
GEN=/nesi/nobackup/nesi00213/RunFolder/hpc3_submit/run_hf_command.py
HF_SIM=/nesi/project/nesi00213/Environments/mrd87_4/workflow/workflow/calculation/hf_sim.py
MODEL=/nesi/project/nesi00213/VelocityModel/Mod-1D/Cant1D_v3-midQ_OneRay.1d
SIM_BIN=/nesi/project/nesi00213/tools/hb_high_binmod_v5.4.5.3
OUT=${1:-hf_commands.txt}
LOG=${OUT%.txt}_generation.log

export gmsim=/nesi/project/nesi00213/Environments/mrd87_4
source "$gmsim/py311/bin/activate"

: > "$OUT"
{
    echo "# generated $(date '+%Y-%m-%d %H:%M:%S') on $(hostname)"
    echo "# generator $GEN (md5 $(md5sum < "$GEN" | cut -c1-32))"
    echo "# appended: --hf_vel_mod_1d $MODEL --sim_bin $SIM_BIN"
} > "$LOG"

ERR=$(mktemp)
trap 'rm -f "$ERR"' EXIT
n=0
for rel in "$R/HikWgtnmax" $(ls -d "$R"/HikWgtnmax_REL* | sort); do
    name=$(basename "$rel")
    cmd=$(python "$GEN" "$rel" 2> "$ERR" | tail -n 1)
    sed "s|^|$name: |" "$ERR" >> "$LOG"

    expected_start="srun --quit-on-interrupt --kill-on-bad-exit=1 python $HF_SIM "
    [[ "$cmd" == "$expected_start"* ]] || { echo "$name: unexpected command: $cmd" >&2; exit 1; }
    [[ "$cmd" == *" $rel/HF/Acc/HF.bin "* ]] || { echo "$name: output is not $rel/HF/Acc/HF.bin" >&2; exit 1; }
    [[ "$cmd" == *"--slip "*"/Stoch/$name.stoch"* ]] || { echo "$name: slip is not $name.stoch" >&2; exit 1; }
    [[ "$cmd" != *"--hf_vel_mod_1d"* && "$cmd" != *"--sim_bin"* ]] || { echo "$name: already names a model or binary" >&2; exit 1; }

    echo "$cmd --hf_vel_mod_1d $MODEL --sim_bin $SIM_BIN" >> "$OUT"
    n=$((n + 1))
done
echo "wrote $n commands to $OUT (generator messages in $LOG)"
