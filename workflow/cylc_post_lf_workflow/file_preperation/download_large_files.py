import argparse
import subprocess

def main():
    parser = argparse.ArgumentParser(description="Download large files from Dropbox using rclone")
    parser.add_argument("version", help="Version identifier (e.g., v25p11)")
    parser.add_argument("fault_name", help="Fault name (e.g., WhiteCk)")
    args = parser.parse_args()

    version = args.version
    fault_name = args.fault_name

    dropbox_source_base = f"dropbox:/QuakeCoRE/gmsim_scratch/{version}"
    dropbox_lf = f"{dropbox_source_base}/LF/{fault_name}"
    dropbox_sources_tar = f"{dropbox_source_base}/Sources/{fault_name}.tar"
    dropbox_vm_h5 = f"{dropbox_source_base}/VMs/HDF5/{fault_name}_velocity_model.h5"

    local_large_temp_tar_dir_base = f"/scratch/projects/rch-quakecore/Cybershake/setup_files_from_dropbox/{version}/large_temp_files/tar/{version}"
    local_lf_tar_dir = f"{local_large_temp_tar_dir_base}/LF/{fault_name}" 
    local_sources_tar_dir = f"{local_large_temp_tar_dir_base}/Sources"
    local_vm_h5_dir = f"{local_large_temp_tar_dir_base}/HDF5"

    # print(f"Trying to clone {dropbox_lf} to {local_lf_tar_dir}")
    # subprocess.run(["rclone", "copy", dropbox_lf, local_lf_tar_dir, "--progress"], check=True)

    # print(f"Trying to clone {dropbox_sources_tar} to {local_sources_tar_dir}")
    # subprocess.run(["rclone", "copy", dropbox_sources_tar, local_sources_tar_dir, "--progress"], check=True)

    print(f"Trying to clone {dropbox_vm_h5} to {local_vm_h5_dir}")
    subprocess.run(["rclone", "copy", dropbox_vm_h5, local_vm_h5_dir, "--progress"], check=True)

if __name__ == "__main__":
    main()