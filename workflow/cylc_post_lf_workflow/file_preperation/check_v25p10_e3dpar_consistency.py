from pathlib import Path
import os
from natsort import natsorted

# Parameters that are known/allowed to differ between the two directory sets
ALLOWED_DIFF_PARAMS = {
    "maxmem",
    "restart_itinc",
    "seisdir",
    "user_scratch",
    "wcc_prog_dir",
    "read_restart"
}

dir1_base = Path("/home/arr65/data/Cybershake_mock_dir_structure/check_v25p10_e3dpar_consistency/v25p10_configs_params")
dir2_base = Path("/home/arr65/data/Cybershake_mock_dir_structure/check_v25p10_e3dpar_consistency/v25p10_e3dpar")

all_fault_names1 = [f for f in os.listdir(dir1_base / Path("Runs")) if not f.endswith(".yaml")]
all_fault_names2 = [f for f in os.listdir(dir2_base / Path("Runs")) if not f.endswith(".yaml")]

if set(all_fault_names1) != set(all_fault_names2):
    only_in_1 = set(all_fault_names1) - set(all_fault_names2)
    only_in_2 = set(all_fault_names2) - set(all_fault_names1)
    if only_in_1:
        print(f"Fault names only in dir1: {sorted(only_in_1)}")
    if only_in_2:
        print(f"Fault names only in dir2: {sorted(only_in_2)}")
assert set(all_fault_names1) == set(all_fault_names2), "Fault names in both directories do not match."

for fault_name in natsorted(all_fault_names1):

    realisation_names1 = [f for f in os.listdir(dir1_base / Path(f"Runs/{fault_name}")) if not f.endswith(".yaml")]
    realisation_names2 = [f for f in os.listdir(dir2_base / Path(f"Runs/{fault_name}")) if not f.endswith(".yaml")]

    if set(realisation_names1) != set(realisation_names2):
        only_in_1 = set(realisation_names1) - set(realisation_names2)
        only_in_2 = set(realisation_names2) - set(realisation_names1)
        print(f"Realisation mismatch for fault '{fault_name}':")
        print(f"  dir1 ({dir1_base / 'Runs' / fault_name}): {sorted(realisation_names1)}")
        print(f"  dir2 ({dir2_base / 'Runs' / fault_name}): {sorted(realisation_names2)}")
        if only_in_1:
            print(f"  Only in dir1: {sorted(only_in_1)}")
        if only_in_2:
            print(f"  Only in dir2: {sorted(only_in_2)}")
    assert set(realisation_names1) == set(realisation_names2), f"Realisation names for fault {fault_name} do not match."

    for realisation_name in natsorted(realisation_names1):

        path1_to_e3dpar = dir1_base / Path(f"Runs/{fault_name}/{realisation_name}/LF/e3d.par")
        path2_to_e3dpar = dir2_base / Path(f"Runs/{fault_name}/{realisation_name}/LF/e3d.par")

        if not path1_to_e3dpar.exists() or not path2_to_e3dpar.exists():
            continue

        with open(path1_to_e3dpar, 'r') as file1, open(path2_to_e3dpar, 'r') as file2:
            content1 = file1.read()
            content2 = file2.read()

            if content1 != content2:
                # Parse key=value pairs and show parameter-level differences
                def parse_params(text):
                    params = {}
                    for line in text.splitlines():
                        line = line.strip()
                        if "=" in line and not line.startswith("#"):
                            key, _, value = line.partition("=")
                            params[key.strip()] = value.strip()
                    return params

                params1 = parse_params(content1)
                params2 = parse_params(content2)
                all_keys = set(params1) | set(params2)
                unexpected_diffs = [
                    key for key in sorted(all_keys)
                    if key not in ALLOWED_DIFF_PARAMS and (
                        key not in params1 or key not in params2 or params1[key] != params2[key]
                    )
                ]
                if unexpected_diffs:
                    print("Unexpected parameter-level differences:")
                    for key in unexpected_diffs:
                        if key not in params1:
                            print(f"  {key}: (missing in dir1)  ->  {params2[key]}")
                        elif key not in params2:
                            print(f"  {key}: {params1[key]}  ->  (missing in dir2)")
                        else:
                            print(f"  {key}: {params1[key]}  ->  {params2[key]}")

                assert not unexpected_diffs, f"Contents of e3d.par do not match for fault {fault_name} and realisation {realisation_name}."

print("All e3d.par files are consistent.")

