#!/bin/bash
#SBATCH --account=nesi00213
#SBATCH --job-name=fetch_HikWgtnmax_VM
#SBATCH --partition=milan,genoa
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=1-00:00:00
#SBATCH --output=/home/arr65/hikwgtnmax_v26p5/logs/fetch_vm_%j.out
#SBATCH --error=/home/arr65/hikwgtnmax_v26p5/logs/fetch_vm_%j.err
#
# Copy HikWgtnmax's Vs velocity-model file (vs3dfile.s, 179901414720 bytes) from
# Dropbox, where Sung uploaded the v26p5 VM on 2026-05-15, to the VM folder on
# NeSI, which so far holds only the VM parameters.
#
# The old bb_sim reads vs3dfile.s[y, 0, x] * 1000 at each LF station for the
# lf_vs_ref header field (with --no-lf-amp it does not affect the waveforms).
# It also reads vm_params.yaml, which is already there and byte-identical to
# Dropbox's (md5 ab7839ee174ead739f67d4f8eafbbb4a). The model matches the LF
# run's e3d.par: nx 6596, ny 9882, nz 690, h 0.1, modelrot 18.764847251107316.
#
# Read-only on Dropbox: rclone copy only reads the source. Copies only
# vs3dfile.s and refuses to start if a vs3dfile.s of another size is already
# there. Safe to resubmit: an identical file is skipped, and rclone downloads to
# a .partial name first. The final rclone check compares size and Dropbox
# content hash with the source.

set -euo pipefail

SRC=dropbox:/QuakeCoRE/gmsim_scratch/v26p5/VMs/HikWgtnmax
DEST=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Data/VMs/HikWgtnmax
FILE=vs3dfile.s
SIZE=179901414720
LOG=/home/arr65/hikwgtnmax_v26p5/logs/rclone_vm_${SLURM_JOB_ID}.log

module load rclone/1.74.3 2>/dev/null || true
command -v rclone > /dev/null || { echo "rclone not found"; exit 1; }

echo "job     : ${SLURM_JOB_ID} on $(hostname)"
echo "rclone  : $(rclone version | head -1)"
echo "source  : $SRC/$FILE"
echo "dest    : $DEST"
echo "log     : $LOG"
echo "started : $(date '+%F %T')"

if [[ -e "$DEST/$FILE" && "$(stat -c %s "$DEST/$FILE")" != "$SIZE" ]]; then
    echo "$DEST/$FILE exists with another size - refusing to overwrite it"
    exit 1
fi
if [[ "$(rclone lsf --format s "$SRC/$FILE")" != "$SIZE" ]]; then
    echo "$SRC/$FILE is not the expected $SIZE bytes - stopping"
    exit 1
fi

rclone copy "$SRC" "$DEST" --include "/$FILE" \
    --multi-thread-streams 8 --tpslimit 10 \
    --retries 10 --low-level-retries 20 \
    --stats 10m --stats-one-line \
    --log-level INFO --log-file "$LOG"
echo "copied  : $(date '+%F %T')"

rclone check "$SRC" "$DEST" --include "/$FILE" --one-way --log-level INFO --log-file "$LOG"
echo "checked : $(date '+%F %T') - $FILE present, size and hash match the source"
