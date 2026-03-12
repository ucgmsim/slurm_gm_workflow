from pathlib import Path
import os
from natsort import natsorted

def parse_params(text):
    params = {}
    for line in text.splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            key, _, value = line.partition("=")
            params[key.strip()] = value.strip()
    return params


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

allowed_param_values = {param: set() for param in ALLOWED_DIFF_PARAMS}

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

            params1 = parse_params(content1)
            params2 = parse_params(content2)

            for key in ALLOWED_DIFF_PARAMS:
                if key in params1:
                    allowed_param_values[key].add(params1[key])
                if key in params2:
                    allowed_param_values[key].add(params2[key])

            if content1 != content2:
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

# Collect unique base path prefixes from ALL parameters across both file sets
# A "base path prefix" is everything up to and including the directory before "Cybershake/"
import re

all_base_paths_dir1 = set()
all_base_paths_dir2 = set()

for fault_name in natsorted(all_fault_names1):
    for realisation_name in natsorted(os.listdir(dir1_base / f"Runs/{fault_name}")):
        if realisation_name.endswith(".yaml"):
            continue
        for label, base, dest_set in [
            ("dir1", dir1_base, all_base_paths_dir1),
            ("dir2", dir2_base, all_base_paths_dir2),
        ]:
            par_path = base / f"Runs/{fault_name}/{realisation_name}/LF/e3d.par"
            if not par_path.exists():
                continue
            with open(par_path) as f:
                for line in f:
                    # Extract path-like values (quoted or unquoted)
                    m = re.search(r'=\s*"?(/[^"]+)"?\s*$', line.strip())
                    if m:
                        path_val = m.group(1)
                        # Extract prefix before /Cybershake/
                        idx = path_val.find("/Cybershake/")
                        if idx != -1:
                            dest_set.add(path_val[:idx + len("/Cybershake")])

print("\n--- Unique base path prefixes (up to .../Cybershake) ---")
print(f"  dir1 ({dir1_base.name}):")
for p in sorted(all_base_paths_dir1):
    print(f"    {p}")
print(f"  dir2 ({dir2_base.name}):")
for p in sorted(all_base_paths_dir2):
    print(f"    {p}")

only_in_dir2 = all_base_paths_dir2 - all_base_paths_dir1
if only_in_dir2:
    print(f"\n  NEW base paths (in dir2 only):")
    for p in sorted(only_in_dir2):
        print(f"    {p}")

print("\nUnique values for non-path allowed-diff parameters:")
for param in sorted(ALLOWED_DIFF_PARAMS):
    vals = allowed_param_values[param]
    # Skip params whose values are all paths — those are covered above
    non_path_vals = {v for v in vals if not v.strip('"').startswith("/")}
    if non_path_vals:
        print(f"  {param}: {sorted(vals)}")

