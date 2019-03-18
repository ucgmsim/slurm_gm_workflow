#!/usr/bin/env python
"""
Checks that a given IM_calc folder has all csv files expected after IM_calculation has run for a simulated or observed
event.
Example usage:
python im_calc_checkpoint.py /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_rerun/Runs/Kelly/Kelly_HYP01-29_S1244/IM_calc 5302
"""

import os
import sys
import glob
import argparse

IM_CALC_DIR = "IM_calc"
CSV_PATTERN = "*.csv"
CSV_SUFFIX = ".csv"
META_PATTERN = "*imcalc.info"
# Examples:
# sim_waveform_dirs =
# ['/nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6/Runs/test/Kelly/BB/Cant1D_v3-midQ_OneRay_hfnp2mm+_rvf0p8_sd50_k0p045/Kelly_HYP20-29_S1434',
# '/nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6/Runs/test/Kelly/BB/Cant1D_v3-midQ_OneRay_hfnp2mm+_rvf0p8_sd50_k0p045/Kelly_HYP29-29_S1524',
# '/nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6/Runs/test/Kelly/BB/Cant1D_v3-midQ_OneRay_hfnp2mm+_rvf0p8_sd50_k0p045/Kelly_HYP07-29_S1304']
# dire = '/nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6/Runs/test/Kelly/BB/Cant1D_v3-midQ_OneRay_hfnp2mm+_rvf0p8_sd50_k0p045/Kelly_HYP20-29_S1434'
# output_sim_dir = /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6/Runs/test/Kelly/BB/Cant1D_v3-midQ_OneRay_hfnp2mm+_rvf0p8_sd50_k0p045/Kelly_HYP20-29_S1434/../../../IM_calc/Kelly_HYP20-29_S1434/


def print_verbose(message, verbose):
    if verbose:
        print(message)


def check_im_calc_completion(output_dir, station_count, verbose=False, event_name=None):
    """
    Check for any csv files in the given folder, or stations folder inside.
    Check for any imcalc.info files.
    If at least one of each type are present return True, otherwise return False
    :param output_dir: The path to the IM_calc directory
    :param station_count: The number of expected stations
    :param event_name: The name of the event being checked. If not none assumes event was observed, rather than simulated.
    :param verbose: Print extra information or not
    :return: True if all conditions for job completion are true, false otherwise
    """
    if os.path.isdir(output_dir):  # if output dir exists

        sum_csv = glob.glob1(output_dir, CSV_PATTERN)
        if len(sum_csv) > 0 and station_count > 0:
            if event_name:
                sum_csv = [f for f in sum_csv if event_name in f]
                if not sum_csv:
                    print_verbose("No csv file found", verbose)
                    return False

            csv_file_name = sum_csv[0]

            with open(os.path.join(output_dir, csv_file_name)) as csv_file:
                if station_count != (len(csv_file.readlines()) - 1):
                    print_verbose("CSV file does not have enough lines in it", verbose)
                    return False

        station_dir = os.path.join(output_dir, "stations")
        if os.path.isdir(station_dir) and not event_name:
            station_files = glob.glob1(station_dir, CSV_PATTERN)
            if 0 < station_count != len(station_files):
                print_verbose("Not enough files in the station directory", verbose)
                return False
            sum_csv = sum_csv + station_files
        meta = glob.glob1(output_dir, META_PATTERN)
        # if sum_csv and meta are not empty lists('.csv' and '_imcalc.info' files present)
        # then we think im calc on the corresponding dir is completed and hence remove
        print_verbose("Checking csv and meta files exist", verbose)
        return sum_csv != [] and meta != []
    print_verbose("Output directory does not exist", verbose)
    return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "run_dir", type=str, help="The path to the realisation directory"
    )
    parser.add_argument(
        "station_count",
        type=int,
        help="The number of stations in the realisation",
        nargs="?",
        default=-1,
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="flag to echo messages"
    )
    parser.add_argument(
        "-e",
        "--event_name",
        help="Name of the event being checked. If given it is assumed that the event was observed, rather than simulated",
        default=None,
    )

    args = parser.parse_args()

    res = check_im_calc_completion(
        args.run_dir, args.station_count, args.verbose, args.event_name
    )
    if res:
        print_verbose(
            "{} passed".format(args.event_name if args.event_name else args.run_dir),
            args.verbose,
        )
        sys.exit(0)
    else:
        print_verbose(
            "{} failed".format(args.event_name if args.event_name else args.run_dir),
            args.verbose,
        )
        sys.exit(1)
