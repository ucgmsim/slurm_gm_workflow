#!/usr/bin/env python3

import sys
import argparse
import numpy as np

from qcore.timeseries import BBSeis
from qcore.timeseries import HFSeis

# the ratio of allowed zero's before being flagged as failed, 0.01 = 1%
ZERO_COUNT_THRESHOLD = 0.01

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("bin", type=str)
    parser.add_argument("fd_ll", type=str)
    parser.add_argument(
        "process_type", type=str, choices=["bb", "hf"], help="Either bb or hf"
    )
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
            print("Cannot read binary file {} {}".format(args.bin, ex))
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
    # Removes leading 0s from the test as there may be some time at the start before the waveform starts.
    for stat_name in np.random.choice(
        bin.stations.name, replace=False, size=min(10, bin.stations.shape[0])
    ):
        acc = bin.acc(stat_name)
        for comp in acc.T:
            # trim leading and trailing zeros
            comp_trimmed = np.trim_zeros(comp)
            if comp_trimmed.size == 0:
                if args.verbose:
                    print(
                        f" The waveform for station {stat_name} contains all zeros, please investigate."
                    )
                sys.exit(1)
            ratio_zeros = (
                comp_trimmed.size - np.count_nonzero(comp_trimmed)
            ) / comp_trimmed.size
            if ratio_zeros > ZERO_COUNT_THRESHOLD:
                if args.verbose:
                    print(
                        f"The waveform for station {stat_name} contains {ratio_zeros} zeros, more than {ZERO_COUNT_THRESHOLD}, please investigate. This "
                        f"is most likely due to crashes during HF or BB resulting in no written output."
                    )
                sys.exit(1)

    # pass both check
    if args.verbose:
        print("{} passed".format(args.process_type))
    sys.exit(0)
