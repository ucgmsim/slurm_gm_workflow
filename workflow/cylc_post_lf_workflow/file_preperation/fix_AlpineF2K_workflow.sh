#!/bin/bash
set -euo pipefail

# Parse --dry-run flag
DRY_RUN=false
for arg in "$@"; do
    if [[ "$arg" == "--dry-run" ]]; then
        DRY_RUN=true
    fi
done

if [[ "$DRY_RUN" == true ]]; then
    echo "*** DRY RUN MODE — no changes will be made ***"
    echo ""
fi

WORKFLOW="wf4/run1"
DROPBOX_PATH="dropbox:/QuakeCoRE/gmsim_scratch/v25p11/IM/AlpineF2K"
RUNS_DIR="/scratch/projects/rch-quakecore/Cybershake/v25p11/Runs/AlpineF2K"
FAULT="AlpineF2K"

# Helper: run or print a command depending on dry-run mode
run_cmd() {
    if [[ "$DRY_RUN" == true ]]; then
        echo "  [DRY RUN] $*"
    else
        "$@"
    fi
}

# Build full list of realizations
REALIZATIONS=("AlpineF2K")
for i in $(seq 1 47); do
    REALIZATIONS+=("AlpineF2K_REL$(printf '%02d' "$i")")
done

# 1. Get completed realizations from Dropbox
echo "=== Checking Dropbox for completed realizations ==="
COMPLETED=()
DROPBOX_LIST=$(rclone ls "$DROPBOX_PATH" 2>/dev/null || true)
for REL in "${REALIZATIONS[@]}"; do
    if echo "$DROPBOX_LIST" | grep -q "${REL}_IM.tar"; then
        COMPLETED+=("$REL")
    fi
done
echo "Found ${#COMPLETED[@]} completed realizations on Dropbox."

# 2. Among non-completed, find failed HF jobs
echo ""
echo "=== Checking for failed HF jobs ==="
FAILED_HF=()
for REL in "${REALIZATIONS[@]}"; do
    if printf '%s\n' "${COMPLETED[@]}" | grep -qx "$REL"; then
        continue
    fi

    HF_DIR="$RUNS_DIR/$REL/HF/Acc"
    HF_OK=true
    for f in HF.bin HF.log SEED; do
        if [[ ! -s "$HF_DIR/$f" ]]; then
            HF_OK=false
            break
        fi
    done

    if [[ "$HF_OK" == false ]]; then
        FAILED_HF+=("$REL")
        echo "  FAILED HF: $REL"
    else
        echo "  HF OK:     $REL"
    fi
done
echo "Found ${#FAILED_HF[@]} realizations with failed HF."

if [[ ${#FAILED_HF[@]} -eq 0 ]]; then
    echo "No failed HF jobs found. Nothing to do."
    exit 0
fi

# 3. Delete partial BB and IM output for failed HF realizations
echo ""
echo "=== Deleting partial BB/IM output ==="
for REL in "${FAILED_HF[@]}"; do
    for STAGE in BB IM; do
        DIR="$RUNS_DIR/$REL/$STAGE"
        if [[ -d "$DIR" ]]; then
            run_cmd rm -rf "$DIR"
        fi
    done
done

# 4. Start the workflow so cylc commands work
echo ""
echo "=== Starting workflow ==="
run_cmd cylc play "$WORKFLOW"

if [[ "$DRY_RUN" == false ]]; then
    echo "Waiting for scheduler to start..."
    sleep 5
fi

# 5. Mark HF tasks as failed and trigger them with a new flow
#    --flow=new starts a new flow from the HF task, which will
#    cascade through BB -> IM -> upload -> delete via the graph
echo ""
echo "=== Marking HF tasks as failed and triggering re-run ==="
for REL in "${FAILED_HF[@]}"; do
    echo "  Processing: run_hf-${FAULT}-${REL}"
    run_cmd cylc set --out=failed "${WORKFLOW}//${1:-1}/run_hf-${FAULT}-${REL}"
    run_cmd cylc trigger --flow=new "${WORKFLOW}//${1:-1}/run_hf-${FAULT}-${REL}"
done

echo ""
echo "=== Done ==="
echo "Processed ${#FAILED_HF[@]} failed HF realizations."
echo "Each HF task will re-run and cascade through BB -> IM -> upload -> delete."
if [[ "$DRY_RUN" == true ]]; then
    echo "*** This was a dry run. Re-run without --dry-run to apply changes. ***"
fi