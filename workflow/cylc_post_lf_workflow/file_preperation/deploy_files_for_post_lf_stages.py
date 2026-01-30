from pathlib import Path
import shutil

def create_modified_config_file(original_file_path, modified_file_path, old_base_path, new_base_path, fixed_value_overrides=None):
    """
    Read original config file, modify paths, and write to modified_file_path.
    
    First replaces all occurrences of old_base_path with new_base_path, 
    then applies any fixed value overrides for key=value or key: value lines.
    
    Parameters
    ----------
    original_file_path : Path or str
        Path to the original config file
    modified_file_path : Path or str
        Path to write the modified config file
    old_base_path : str
        Old base directory path to replace
    new_base_path : Path or str
        New base directory to replace the old path with
    fixed_value_overrides : dict, optional
        Dictionary of {key: value} pairs to override specific keys in the config file.
        Supports both '=' delimiter (.par files) and ':' delimiter (YAML files).
    """
    with open(original_file_path, 'r') as f:
        content = f.read()
    
    # First, replace all occurrences of the old base path with the new one
    modified_content = content.replace(old_base_path, str(new_base_path))
    
    # If there are fixed value overrides, process line by line to apply them
    if fixed_value_overrides:
        modified_lines = []
        for line in modified_content.splitlines(keepends=True):
            stripped = line.strip()
            
            # Skip empty lines and comments
            if not stripped or stripped.startswith('#'):
                modified_lines.append(line)
                continue
            
            # Determine the delimiter and extract the key
            delimiter = None
            key = None
            
            if '=' in stripped:
                delimiter = '='
                key = stripped.split('=', 1)[0].strip()
            elif ':' in stripped:
                delimiter = ': '
                key = stripped.split(':', 1)[0].strip()
            
            # Check if this key needs a fixed value override
            if key and key in fixed_value_overrides:
                # Preserve leading whitespace (indentation) for YAML files
                leading_whitespace = line[:len(line) - len(line.lstrip())]
                modified_lines.append(f"{leading_whitespace}{key}{delimiter}{fixed_value_overrides[key]}\n")
                continue
            
            modified_lines.append(line)
        
        modified_content = ''.join(modified_lines)
    
    # Ensure the output directory exists
    modified_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(modified_file_path, 'w') as f:
        f.write(modified_content)


# Fixed value overrides for e3d.par files
E3D_PAR_FIXED_VALUES = {
    "wcc_prog_dir": '"/scratch/projects/rch-quakecore/EMOD3D_old_Cybershake/tools/emod3d-mpi_v3.0.8"',
    "vel_mod_params_dir": '"/scratch/projects/rch-quakecore/Cybershake/v25p11/Data/VMs/MS09"',
    "grid_file": '""',
    "model_params": '""',
}

cybershake_base_dir = Path("/home/arr65/data/Cybershake_mock_dir_structure/Cybershake")

version = "v25p11"
fault = "MS09"

realizations = ["MS09",
                "MS09_REL01", 
                "MS09_REL02",
                "MS09_REL03",
                "MS09_REL04",
                "MS09_REL05",
                "MS09_REL06",
                "MS09_REL07",
                "MS09_REL08",
                "MS09_REL09",
                "MS09_REL10",
                "MS09_REL11",
                "MS09_REL12",
                "MS09_REL13",
                "MS09_REL14",
                "MS09_REL15",
                "MS09_REL16",
                "MS09_REL17",
                "MS09_REL18",
                "MS09_REL19",
                "MS09_REL20",
                "MS09_REL21",
                "MS09_REL22",
                "MS09_REL23",
                "MS09_REL24",
                "MS09_REL25",
                "MS09_REL26",
                "MS09_REL27"]


# realizations = ["MS09",
#                 "MS09_REL01"]

base_cybershake_dir = Path("/home/arr65/data/Cybershake_mock_dir_structure/Cybershake")
old_base_path_to_replace = "/uoc/project/uoc40001/scratch/baes/Cybershake"
rch_base_path = Path("/scratch/projects/rch-quakecore/Cybershake")

# =============================================================================
# Operations that depend only on version (not fault or realization)
# =============================================================================

# Create modified root_params.yaml file
original_root_params_file_path = (base_cybershake_dir / "setup_files_from_dropbox"/ version / 
                              "extracted_original_setup_files_from_dropbox" / version / 
                              f"{version}_configs_params"  / "root_params.yaml")

modified_root_params_file_path = base_cybershake_dir / version / "Runs" / "root_params.yaml"

create_modified_config_file(original_file_path=original_root_params_file_path, 
                            modified_file_path=modified_root_params_file_path, 
                            old_base_path=old_base_path_to_replace, 
                            new_base_path=rch_base_path,
                            fixed_value_overrides={
                                "hf_vel_mod_1d": "/scratch/projects/rch-quakecore/Cybershake/VelocityModel/Mod-1D/Cant1D_v3-midQ_OneRay.1d",
                                "mgmt_db_location": ""}
                            )

# copy .ll and .vs30 files
original_ll_and_vs30_source_path = (base_cybershake_dir / "setup_files_from_dropbox"/ version / 
                                    "extracted_original_setup_files_from_dropbox" / version / 
                                    "VMs" / f"{version}_setup_files")

destination_ll_and_vs30_path = base_cybershake_dir / version  

shutil.copy(original_ll_and_vs30_source_path/"non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.ll", destination_ll_and_vs30_path)
shutil.copy(original_ll_and_vs30_source_path/"non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.vs30", destination_ll_and_vs30_path)

# =============================================================================
# Operations that depend on fault (but not realization)
# =============================================================================

# Create modified fault_params.yaml file
original_fault_params_file_path = (base_cybershake_dir / "setup_files_from_dropbox"/ version / 
                              "extracted_original_setup_files_from_dropbox" / version / 
                              f"{version}_configs_params"  / fault / "fault_params.yaml")

destination_fault_params_base_base = base_cybershake_dir / version / "Runs" / fault 
modified_fault_params_file_path = destination_fault_params_base_base / "fault_params.yaml"

create_modified_config_file(original_file_path=original_fault_params_file_path, 
                            modified_file_path=modified_fault_params_file_path, 
                            old_base_path=old_base_path_to_replace, 
                            new_base_path=rch_base_path)

# copy ll and statscords
original_ll_statcords_source_path = (base_cybershake_dir / "setup_files_from_dropbox"/ version / 
                                                 "extracted_original_setup_files_from_dropbox" / 
                                                 version / "VMs" / f"{version}_setup_files" / "Runs" / fault)

shutil.copy(original_ll_statcords_source_path/"fd_rt01-h0.100.ll", destination_fault_params_base_base)
shutil.copy(original_ll_statcords_source_path/"fd_rt01-h0.100.statcords", destination_fault_params_base_base)

# Copy Sources
original_source_files_source_path = (base_cybershake_dir / "setup_files_from_dropbox"/ version / 
                                    "extracted_original_setup_files_from_dropbox" / version / 
                                    "Sources" / fault / fault )

destination_source_files_path = base_cybershake_dir / version / "Data" / "Sources" / fault
shutil.copytree(original_source_files_source_path, destination_source_files_path)

# Copy VMs
original_vms_source_path = (base_cybershake_dir / "setup_files_from_dropbox"/ version / 
                                    "extracted_original_setup_files_from_dropbox" / version / 
                                    "VMs" / "VMs_meta_data" / fault )

destination_vms_path = base_cybershake_dir / version / "Data" / "VMs" / fault
shutil.copytree(original_vms_source_path, destination_vms_path)

create_modified_config_file(original_file_path=destination_vms_path / "vm_params.yaml",
                            modified_file_path=destination_vms_path / "vm_params.yaml", 
                            old_base_path="/scratch/hpc91a02/UC/RunFolder/Cybershake/v23p7", 
                            new_base_path=rch_base_path / version)

# Copy addtional VM files that were not included in Dropbox
original_additional_vm_files_source_dir_path = base_cybershake_dir / "VMs_from_cascade" / "VMs" / fault  

shutil.copy(original_additional_vm_files_source_dir_path / "vs3dfile.s", destination_vms_path)
shutil.copy(original_additional_vm_files_source_dir_path / "vp3dfile.p", destination_vms_path)
shutil.copy(original_additional_vm_files_source_dir_path / "rho3dfile.d", destination_vms_path)


# =============================================================================
# Operations that depend on realization
# =============================================================================

for realization in realizations:

    lf_output_source_path = (base_cybershake_dir / "setup_files_from_dropbox" / version / 
                            "extracted_original_setup_files_from_dropbox" / version / "LF" / 
                            fault / f"{realization}_LF_OutBin" / realization / "LF" ) 

    lf_output_destination_path = base_cybershake_dir / version / "Runs" / fault/ realization /"LF"
    shutil.copytree(lf_output_source_path, lf_output_destination_path)

    # Create modified e3d.par file
    original_e3d_par_file_path = (base_cybershake_dir / "setup_files_from_dropbox"/ version / 
                                  "extracted_original_setup_files_from_dropbox" / version / 
                                  f"{version}_configs_params"  / fault/realization/"LF" / "e3d.par")
    
    modified_e3d_par_file_path = base_cybershake_dir / version / "Runs" / fault /realization / "LF" / "e3d.par"
    
    create_modified_config_file(original_file_path=original_e3d_par_file_path, 
                                modified_file_path=modified_e3d_par_file_path, 
                                old_base_path=old_base_path_to_replace, 
                                new_base_path=rch_base_path, 
                                fixed_value_overrides=E3D_PAR_FIXED_VALUES)
    
    # Create modified sim_params.yaml file
    original_sim_params_file_path = (base_cybershake_dir / "setup_files_from_dropbox"/ version / 
                                  "extracted_original_setup_files_from_dropbox" / version / 
                                  f"{version}_configs_params"  / fault/realization/"sim_params.yaml")
    
    modified_sim_params_file_path = base_cybershake_dir / version / "Runs" / fault /realization / "sim_params.yaml"

    
    create_modified_config_file(original_file_path=original_sim_params_file_path, 
                                modified_file_path=modified_sim_params_file_path, 
                                old_base_path=old_base_path_to_replace, 
                                new_base_path=rch_base_path)
    