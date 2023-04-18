"""
Script to edit a single value in a pyvenv.cfg file.
Each line has the following format:
<key> = <value>
Example use:
python edit_cfg.py pyvenv.cfg include-system-site-packages true
"""
import argparse
from pathlib import Path


def load_args():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("cfg_file", type=Path)
    parser.add_argument("variable_name")
    parser.add_argument("variable_value")
    args = parser.parse_args()
    return args


def main():
    args = load_args()

    data = {}
    with open(args.cfg_file, "r") as cfg:
        for line in cfg.readlines():
            if line.strip():
                key, value = line.strip().split("=")
                data[key.strip()] = value.strip()

    data[args.variable_name] = args.variable_value

    with open(args.cfg_file, "w") as cfg:
        for key, value in data.items():
            cfg.write(f"{key} = {value}\n")


if __name__ == "__main__":
    main()
