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
        "--seed", type=str, default=0, help="The seed to be used for HF simulations. Default is to request a random seed."
    )

    args = parser.parse_args()

    faults = {}
    with open(args.fault_selection_list) as fault_file:
        for line in fault_file.readlines():
            fault, count, *_ = line.split(" ")
            count = int(count[:-2])
            faults.update({fault: count})

    for fault, count in faults.items():
        install_fault(fault, count, args.path_cybershake, args.version, args.seed)


if __name__ == '__main__':
    main()
