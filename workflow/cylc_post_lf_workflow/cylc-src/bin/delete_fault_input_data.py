import argparse
import os
import shutil

def main():
    parser = argparse.ArgumentParser(description="Cleanup input data for a given version and fault name")
    parser.add_argument("version", help="Version of the simulation")
    parser.add_argument("fault_name", help="Fault name")
    args = parser.parse_args()


    base_dir = f"/scratch/projects/rch-quakecore/Cybershake/{args.version}"
    staged_for_uploads_base_dir = f"/scratch/projects/rch-quakecore/Cybershake/staged_for_upload/{args.version}"

    base_dirs_to_delete = [
        f"{base_dir}/Data/Sources",
        f"{base_dir}/Data/VMs",
        f"{base_dir}/Runs",
        f"{staged_for_uploads_base_dir}/BB",
        f"{staged_for_uploads_base_dir}/HF",
        f"{staged_for_uploads_base_dir}/IM",
    ]

    for dir in base_dirs_to_delete:

        dir_to_delete = os.path.join(dir, args.fault_name)
        print(f"Deleting directory: {dir_to_delete}")
        shutil.rmtree(dir_to_delete)

    print("Cleanup completed")

if __name__ == "__main__":
    main()