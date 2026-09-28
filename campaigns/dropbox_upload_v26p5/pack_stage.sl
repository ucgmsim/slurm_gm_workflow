#!/bin/bash
#SBATCH --account=uc04357
#SBATCH --job-name=pack_v26p5
#SBATCH --partition=milan,genoa
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=12:00:00
#SBATCH --output=/home/arr65/dropbox_upload_v26p5/logs/pack_%j.out
#SBATCH --error=/home/arr65/dropbox_upload_v26p5/logs/pack_%j.err
#
# Pack one stage's outputs for Dropbox with pack_stage.py, then write each
# fault folder's README.md with make_readme.py. Reads the run folders only and
# writes only under STAGING. Safe to rerun: verified tars are skipped.
# Usage: sbatch pack_stage.sl STAGE TARGETS REPO_COMMIT [STAGING]

set -euo pipefail

STAGE=${1:?stage}
TARGETS=${2:?targets file}
COMMIT=${3:?repo commit the README cites}
STAGING=${4:-/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/dropbox_staging}
HERE=/home/arr65/dropbox_upload_v26p5

source /nesi/project/nesi00213/Environments/mrd87_4/py311/bin/activate
echo "job     : ${SLURM_JOB_ID:-manual} on $(hostname); stage $STAGE; targets $TARGETS; staging $STAGING"
echo "started : $(date '+%F %T')"
status=0
python "$HERE/pack_stage.py" "$STAGE" "$TARGETS" "$STAGING" || status=$?
for fault in $(ls "$STAGING/$STAGE"); do
    python "$HERE/make_readme.py" "$STAGE" "$fault" "$STAGING" "$COMMIT"
done
echo "finished: $(date '+%F %T')"
exit $status
