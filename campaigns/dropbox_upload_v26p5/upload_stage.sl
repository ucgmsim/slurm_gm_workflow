#!/bin/bash
#SBATCH --account=uc04357
#SBATCH --job-name=dropbox_upload
#SBATCH --partition=milan,genoa
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=1-00:00:00
#SBATCH --output=/home/arr65/dropbox_upload_v26p5/logs/upload_%j.out
#SBATCH --error=/home/arr65/dropbox_upload_v26p5/logs/upload_%j.err
#
# Upload one staged Cybershake v26p5 folder, STAGING/<STAGE>/<FAULT>, to
# dropbox:/QuakeCoRE/gmsim_scratch/v26p5/<STAGE>/<FAULT>. The folder holds the
# per-realisation tars pack_stage.py wrote, with MANIFEST.tsv and README.md.
#
# Refuses to start unless:
#   - the folder is complete: README.md present, and every tar in MANIFEST.tsv
#     present at its recorded size, with no stray tar and no .partial leftover;
#   - the manifest checksums its own contents again (sha256sum of each tar).
#
# The copy uses --immutable, so an existing Dropbox file is never modified: a
# same-named file with other content is an error, and an identical one is
# skipped. So a rerun resumes, and nothing on Dropbox is ever deleted.
#
# Afterwards `rclone check --one-way` compares every uploaded file's size and
# Dropbox content hash with the local copy. The job fails unless there are 0
# differences, no file that could not be hashed, and a match for every local
# file (the checks in old_workflow_lf_cleanup's lfc_verify.sh).
#
# Usage: sbatch upload_stage.sl STAGE FAULT [--dry-run]
# (--dry-run lists what would be copied, and copies and checks nothing)

set -euo pipefail

STAGE=${1:?stage}
FAULT=${2:?fault}
DRY=${3:-}
# overridable only to test this script against a local directory
STAGING=${STAGING:-/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/dropbox_staging}
DEST_ROOT=${DEST_ROOT:-dropbox:/QuakeCoRE/gmsim_scratch/v26p5}
LOGDIR=${LOGDIR:-/home/arr65/dropbox_upload_v26p5/logs}
SRC=$STAGING/$STAGE/$FAULT
DEST=$DEST_ROOT/$STAGE/$FAULT
LOG=$LOGDIR/rclone_${SLURM_JOB_ID:-manual}_${STAGE}_${FAULT}.log
RECORD=${RECORD:-/home/arr65/dropbox_upload_v26p5/uploads.txt}
FILTER=(--include "/*.tar" --include "/MANIFEST.tsv" --include "/README.md")

module load rclone/1.74.3 2>/dev/null || true
echo "job      : ${SLURM_JOB_ID:-manual} on $(hostname)"
echo "rclone   : $(rclone version | head -1)"
echo "source   : $SRC"
echo "dest     : $DEST"
echo "started  : $(date '+%F %T')"

# --- the staged folder must be complete and self-consistent ---
[[ -f "$SRC/MANIFEST.tsv" && -f "$SRC/README.md" ]] || { echo "MANIFEST.tsv or README.md missing in $SRC"; exit 1; }
if compgen -G "$SRC/*.partial" > /dev/null; then echo "partial tars in $SRC - pack_stage.py did not finish"; exit 1; fi
python3 - "$SRC" <<'EOF'
import collections, hashlib, os, sys
src = sys.argv[1]
rows = [ln.rstrip("\n").split("\t") for ln in open(os.path.join(src, "MANIFEST.tsv")).readlines()[1:] if ln.strip()]
tars = collections.OrderedDict((r[0], (int(r[1]), r[2])) for r in rows)
local = sorted(f for f in os.listdir(src) if f.endswith(".tar"))
if sorted(tars) != local:
    sys.exit(f"tars in {src} {local} do not match MANIFEST.tsv {sorted(tars)}")
for name, (size, sha) in tars.items():
    path = os.path.join(src, name)
    if os.path.getsize(path) != size:
        sys.exit(f"{name}: {os.path.getsize(path)} bytes, manifest says {size}")
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(64 << 20), b""):
            h.update(block)
    if h.hexdigest() != sha:
        sys.exit(f"{name}: sha256 does not match MANIFEST.tsv")
print(f"staged folder complete: {len(tars)} tars, all at their manifest size and sha256")
EOF
n_local=$(( $(ls "$SRC"/*.tar | wc -l) + 2 ))

rclone copy "$SRC" "$DEST" "${FILTER[@]}" --immutable $DRY \
    --transfers 4 --checkers 8 --tpslimit 10 --dropbox-chunk-size 128M \
    --retries 10 --low-level-retries 20 --stats 10m --stats-one-line \
    --log-level INFO --log-file "$LOG"
if [[ "$DRY" == "--dry-run" ]]; then
    echo "dry run: would copy $(grep -c 'Skipped copy as --dry-run' "$LOG") file(s) - see $LOG"
    exit 0
fi
echo "copied   : $(date '+%F %T')"

out=$(rclone check "$SRC" "$DEST" "${FILTER[@]}" --one-way 2>&1) || { echo "$out"; echo "rclone check FAILED"; exit 1; }
echo "$out" | tail -n 4
count() { echo "$out" | sed -n "s/.*[^0-9]\([0-9][0-9]*\) $1.*/\1/p" | tail -1; }
unhashed=$(count "hashes could not be checked"); unhashed=${unhashed:-0}
matched=$(count "matching files"); matched=${matched:-0}
if [[ "$unhashed" != 0 || "$matched" != "$n_local" ]]; then
    echo "check incomplete: $matched of $n_local files matched, $unhashed not hashed"
    exit 1
fi
echo "$(date '+%F %T') ${SLURM_JOB_ID:-manual}: $STAGE/$FAULT -> $DEST: $matched files, size and hash verified" | tee -a "$RECORD"
