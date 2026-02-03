import argparse
import os
import shutil

def main():
    parser = argparse.ArgumentParser(description="Cleanup input data for a given version and fault name")
    parser.add_argument("version", help="Version of the simulation")
    parser.add_argument("fault_name", help="Fault name")
    args = parser.parse_args()


    base_dir = f"/scratch/projects/rch-quakecore/Cybershake/{args.version}"

    base_dirs_to_delete = [
        f"{base_dir}/Data/Sources",
        f"{base_dir}/Data/VMs",
        f"{base_dir}/Runs",
    ]

    for dir in base_dirs_to_delete:

        dir_to_delete = os.path.join(dir, args.fault_name)
        print(f"Deleting directory: {dir_to_delete}")
        shutil.rmtree(dir_to_delete)

if __name__ == "__main__":
    main()