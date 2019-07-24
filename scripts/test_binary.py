#!/usr/bin/env python3

import sys
import argparse
import numpy as np

from qcore.timeseries import BBSeis
from qcore.timeseries import HFSeis

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("bin", type=str)
    parser.add_argument("fd_ll", type=str)
    parser.add_argument("process_type", type=str, help="Either bb or hf")
    parser.add_argument("--verbose", action="store_true", default=False)

    args = parser.parse_args()

    if args.process_type.lower() not in ["bb", "hf"]:
        print("Invalid process_type, has to be either bb or hf. Quitting!")
        sys.exit(1)

    bin_class = HFSeis if args.process_type == "hf" else BBSeis

    try:
        bin = bin_class(args.bin)
    except ValueError as ex:
        if args.verbose:
            print("Cannot read binary file {} ".format(args.bin))
        sys.exit(1)

    try:
        f = open(args.fd_ll)
    except Exception as ex:
        if args.verbose:
            print("Cannot open {} with exception\n{}".format(args.fd_ll, ex))
        sys.exit(1)
    else:
        fd_count = len(f.readlines())

    if fd_count != len(bin.stations.name):
        # failed the count check
        if args.verbose:
            print("The station count did not match the fd_ll")
        sys.exit(1)

    # check for empty station names
    for station in bin.stations.name:
        if station == "":
            # failed
            if args.verbose:
                print(
                    "Empty station name detected, {} failed".format(args.process_type)
                )
            sys.exit(1)

    # check for and vs ==0 (failed)
    vs = bin.stations.vsite if args.process_type == "bb" else bin.stations.vs
    if np.min(vs) == 0:
        if args.verbose:
            print("Some vs == 0, {} incomplete".format(args.process_type))
        sys.exit(1)

    # pass both check
    if args.verbose:
        print("{} passed".format(args.process_type))
    sys.exit(0)
