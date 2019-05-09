import argparse
from scripts.cybershake.install_cybershake_fault import install_fault


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path_cybershake",
        type=str,
        help="the path to the root of a specific version cybershake.",
    )
    parser.add_argument(
        "version", type=str, default="16.1", help="Please specify GMSim version"
    )
    parser.add_argument(
        "fault_selection_list", type=str, help="The fault selection file"
    )
    parser.add_argument(
        "--seed", type=str, default=0,
        help="The seed to be used for HF simulations. Default is to request a random seed."
    )
    parser.add_argument(
        "--stat_file_path", type=str,
        default="/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v18p6.ll",
        help="The path to the station info file path."
    )
    parser.add_argument(
        "--extended_period", action="store_true",
        help="Should IM_calc calculate more psa periods."
    )

    args = parser.parse_args()

    faults = {}
    with open(args.fault_selection_list) as fault_file:
        for line in fault_file.readlines():
            fault, count, *_ = line.split(" ")
            count = int(count[:-2])
            faults.update({fault: count})

    for fault, count in faults.items():
        install_fault(
            fault,
            count,
            args.path_cybershake,
            args.version,
            args.seed,
            args.stat_file_path,
            args.extended_period,
        )


if __name__ == '__main__':
    main()
