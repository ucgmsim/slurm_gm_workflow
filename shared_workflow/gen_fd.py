import argparse
import yaml

from install_shared import generate_fd_files

parser = argparse.ArgumentParser()
parser.add_argument("vm_params_path")
parser.add_argument("stat_file")
parser.add_argument("output_path", default=".")

args = parser.parse_args()

vm_params_dict = yaml.safe_load(open(args.vm_params_path))

generate_fd_files(args.output_path, vm_params_dict, args.stat_file)
