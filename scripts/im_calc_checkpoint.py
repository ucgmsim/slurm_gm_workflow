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


def checkpoint_single(output_dir):
    if os.path.isdir(output_dir):  # if output dir exists

        sum_csv = glob.glob1(output_dir, CSV_PATTERN)
        station_dir = os.path.join(output_dir, 'stations')
        if os.path.isdir(station_dir):
            sum_csv += glob.glob1(station_dir, CSV_PATTERN)
        meta = glob.glob1(output_dir, META_PATTERN)
        # if sum_csv and meta are not empty lists('.csv' and '_imcalc.info' files present)
        # then we think im calc on the corresponding dir is completed and hence remove
        return sum_csv != [] and meta != []


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("run_dir", type=str, help="the path to where Runs located")
    parser.add_argument("--v", action="store_true", help="flag to echo messages")

    args = parser.parse_args()

    res = checkpoint_single(args.run_dir)
    if res:
        if args.v:
            print("%s passed".format(args.run_dir))
        sys.exit(0)
    else:
        if args.v:
            print("%s failed".format(args.run_dir))
        sys.exit(1)
