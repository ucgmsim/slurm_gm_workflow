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
    parser.add_argument("process_type", type=str, choices=["bb", "hf"], help="Either bb or hf")
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

    # binary zero check
    # Checks 10 random stations for any occurances of 0 in the output (aka results have not been written)
    for stat_name in np.random.choice(bin.stations.name, replace=False, size=min(10, bin.stations.shape[0])):

        acc = bin.acc(stat_name)

        if np.any(acc == 0):
            if args.verbose:
                print(
                    f"The velocities for station {stat_name} contains zero/s, please investigate. This "
                    f"is most likely due to crashes during HF or BB resulting in no written output."
                )
            sys.exit(1)

    # pass both check
    if args.verbose:
        print("{} passed".format(args.process_type))
    sys.exit(0)
