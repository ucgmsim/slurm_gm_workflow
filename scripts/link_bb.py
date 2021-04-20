import argparse
from os import path, makedirs
from subprocess import run

from qcore import simulation_structure
from qcore import constants
from shared_workflow.shared_automated_workflow import add_to_queue


def arg_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "source_root_dir", type=str, help="path to previous completed vaildation run"
    )
    parser.add_argument(
        "destination_root_dir", type=str, help="path to the destination root directory"
    )
    parser.add_argument(
        "fault_selection_list", type=str, help="a file that contains list of faults"
    )

    args = parser.parse_args()

    return args


def main(source_root_dir, destination_root_dir, fault_selection_list):
    faults = {}
    with open(fault_selection_list, "r") as fault_file:
        for line in fault_file.readlines():
            fault, count, *_ = line.split()
            count = int(count[:-1])
            faults.update({fault: count})
    for fault, count in faults.items():
        for rel_no in range(count):
            # if rel >1, starts at rel_01
            realisation_name = (
                simulation_structure.get_realisation_name(fault, rel_no + 1)
                if count > 1
                else fault
            )
            src_bb_path = simulation_structure.get_bb_bin_path(
                simulation_structure.get_sim_dir(source_root_dir, realisation_name)
            )
            des_bb_path = simulation_structure.get_bb_bin_path(
                simulation_structure.get_sim_dir(destination_root_dir, realisation_name)
            )
            makedirs(path.dirname(des_bb_path), exist_ok=True)
            cmd = ["ln", "-s", f"{src_bb_path}", f"{des_bb_path}"]
            run(cmd)
            # add update mgmt_db command
            queue_folder = simulation_structure.get_mgmt_db_queue(destination_root_dir)
            run_name = realisation_name
            process_type = constants.ProcessType.BB.value
            status = constants.Status.completed.value
            job_id = None
            add_to_queue(queue_folder, run_name, process_type, status, job_id)


if __name__ == "__main__":
    args = arg_parser()
    main(args.source_root_dir, args.destination_root_dir, args.fault_selection_list)
