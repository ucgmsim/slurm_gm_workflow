#script to quickly check if there is any nan or null in csv
import argparse
import pandas as pd

import os
import sys

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("sim_dir", type=str, help="The path to the realisation directory")
    parser.add_argument("adv_im_model", nargs='+', type=str, help="list of adv_IM models that ran")

    args = parser.parse_args()
    
    failed = []
    for model in args.adv_im_model:
        csv_path = os.path.join(args.sim_dir,"IM_calc/{}.csv".format(model))
        #check file exist first to prevent crash
        try:
            df = pd.read_csv(csv_path)
        except FileNotFoundError:
            failed.append(model)
            print("csv for {} does not exist".format(model))
        else:
            if df.isnull().values.any():
                failed.append(model)
    if len(failed) != 0:
        print("some csv has empty data. models: {}".format(failed))
        sys.exit(1)
    else:
        print("simple check passed, all csv contains data")
        sys.exit(0)
