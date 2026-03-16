"""Check IMs for consecutive zeros across all faults and realizations."""

import shutil
import subprocess
from pathlib import Path

import pandas as pd
from natsort import natsorted
from tqdm import tqdm

VERSION = "v25p11"
RCLONE_BASE = f"dropbox:/QuakeCoRE/gmsim_scratch/{VERSION}/IM"
DOWNLOAD_DIR = Path("/home/arr65/data/cybershake_check")
WORK_DIR = DOWNLOAD_DIR / "work"
OUTPUT_PATH = DOWNLOAD_DIR / f"{VERSION}_consecutive_zeros_report.csv"


def rclone_list(remote_path: str) -> list[str]:
    result = subprocess.run(
        ["rclone", "lsf", remote_path],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip().rstrip("/") for line in result.stdout.splitlines() if line.strip()]


def max_consecutive_zeros(csv_path: Path) -> int:
    df = pd.read_csv(csv_path)
    values = df.select_dtypes(include="number").to_numpy().flatten()
    max_run = current_run = 0
    for v in values:
        if v == 0.0:
            current_run += 1
            max_run = max(max_run, current_run)
        else:
            current_run = 0
    return max_run


def load_completed() -> set[str]:
    if OUTPUT_PATH.exists():
        return set(pd.read_csv(OUTPUT_PATH)["file_name"])
    return set()


def append_result(file_name: str, max_zeros: int) -> None:
    row = pd.DataFrame([{"file_name": file_name, "max_num_consecutive_zeros": max_zeros}])
    write_header = not OUTPUT_PATH.exists()
    row.to_csv(OUTPUT_PATH, mode="a", header=write_header, index=False)


def process_tar(fault: str, tar_name: str) -> None:
    remote_tar = f"{RCLONE_BASE}/{fault}/{tar_name}"
    work_dir = WORK_DIR / tar_name.replace(".tar", "")
    work_dir.mkdir(parents=True, exist_ok=True)
    tar_path = work_dir / tar_name

    print(f"  Downloading {remote_tar} ...")
    subprocess.run(["rclone", "copy", remote_tar, str(work_dir)], check=True)

    print(f"  Extracting {tar_name} ...")
    extract_dir = work_dir / "extracted"
    extract_dir.mkdir(exist_ok=True)
    subprocess.run(["tar", "-xf", str(tar_path), "-C", str(extract_dir)], check=True)

    csvs = list(extract_dir.glob("*.csv"))
    if not csvs:
        print(f"  WARNING: no CSV found in {tar_name}, skipping.")
        shutil.rmtree(work_dir)
        return

    csv_path = csvs[0]
    max_zeros = max_consecutive_zeros(csv_path)
    print(f"  {csv_path.name}: max consecutive zeros = {max_zeros}")
    append_result(csv_path.name, max_zeros)

    shutil.rmtree(work_dir)


def main():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    completed = load_completed()
    print(f"Already completed: {len(completed)} entries")

    faults = natsorted(rclone_list(RCLONE_BASE))
    print(f"Found {len(faults)} faults")

    for fault in faults:
        print(f"\nFault: {fault}")
        tar_files = natsorted(f for f in rclone_list(f"{RCLONE_BASE}/{fault}") if f.endswith(".tar"))
        print(f"  {len(tar_files)} tar file(s)")

        with tqdm(tar_files, unit="realization") as pbar:
            for tar_name in pbar:
                # Derive the expected CSV name from the tar name: e.g. MS09_IM.tar -> MS09.csv
                csv_name = tar_name.replace("_IM.tar", ".csv")
                pbar.set_description(tar_name)
                if csv_name in completed:
                    continue
                process_tar(fault, tar_name)


if __name__ == "__main__":
    main()
