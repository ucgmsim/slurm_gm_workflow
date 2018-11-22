# TODO centre align the last bin

import os
import argparse
import json
import pandas as pd
import matplotlib.pyplot as plt
from qcore import utils
OUT_DIR = 'json_plots'
OUT_NAME = 'run_time.png'


def populate_time_dict(sim_dict, time_dict):
    for sim_type in sim_dict.keys():
        if sim_type != 'common':
            if time_dict.get(sim_type) is None:
                time_dict[sim_type] = []
            try:
                time_dict[sim_type].append(float(sim_dict[sim_type]['run_time'].split()[0]))
            except KeyError:
                continue


def get_run_time_all_sims(all_sims_json):
    with open(all_sims_json, 'r') as f:
        d = json.load(f)
    time_dict = {}
    for sim_dict in d.values():
        populate_time_dict(sim_dict, time_dict)
    return time_dict


def get_run_time(fault_dir):
    json_dir = os.path.join(fault_dir, 'jsons')
    if os.path.isdir(json_dir):
        if os.path.exists(os.path.join(json_dir, 'all_sims.json')):
            print("all sims exists")
            return get_run_time_all_sims(os.path.join(json_dir, 'all_sims.json'))
        else:
            time_dict = {}
            for f in os.listdir(json_dir):
                with open(os.path.join(json_dir, f), 'r') as j:
                    sim_dict = json.load(j)
                populate_time_dict(sim_dict, time_dict)

        return time_dict
    else:
        print("{} does not have a jsons dir".format(fault_dir))


def plot_run_time(stats_dict, fault_dir, out_dir):
    df = pd.DataFrame.from_dict(stats_dict)
   
    axes = df.hist(xlabelsize=8, rwidth=0.5)

    for ax in axes.flatten():
        print ax
        ax.set_xlabel("run time (h)")
        ax.set_ylabel("number of runs")

    out_path = os.path.join(fault_dir, out_dir)
    print(out_path)
    utils.setup_dir(out_path)
    plt.suptitle("{} {}".format(fault_dir.split('/')[-1],"v18p6 run time"))
    plt.savefig(os.path.join(out_path, OUT_NAME))
    plt.show()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder', help='abs path to *.out folder')
    parser.add_argument('-o', '--out_dir', default=OUT_DIR, help="path to save output plot. Default is {}".format(OUT_DIR))
    parser.add_argument('-sf', '--single_fault', action='store_true',
                        help="Please add '-sf' to indicate that run_folder path points to a single fault eg, add '-sf' if run_folder is '/nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_2/Runs/Hollyford'")

    args = parser.parse_args()
    assert os.path.isdir(args.run_folder)

    if args.single_fault:
        stats_dict = get_run_time(args.run_folder)
        plot_run_time(stats_dict, args.run_folder, args.out_dir)
    else:
        for fault in os.listdir(args.run_folder):
            fault_dir = os.path.join(args.run_folder, fault)
            stats_dict = get_run_time(fault_dir)
            plot_run_time(stats_dict, fault_dir, args.out_dir)


if __name__ == '__main__':
    main()

