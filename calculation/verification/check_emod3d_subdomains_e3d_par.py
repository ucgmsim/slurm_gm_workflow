import argparse

from calculation.verification.check_emod3d_subdomains import test_domain
from qcore.utils import load_yaml

parser = argparse.ArgumentParser(allow_abbrev=False)
parser.add_argument("nproc")
parser.add_argument("vm_params")
args = parser.parse_args()

params = load_yaml(args.vm_params)
res = test_domain(params["nx"], params["ny"], params["nz"], args.nproc)
for x in res:
    if x.size > 0:
        exit(1)
