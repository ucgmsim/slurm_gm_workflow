#!/usr/bin/env python3

"""
Clean up (making tar) a single simulation directory after successful cybershake submissions
"""
import os
import glob
import shutil
import tarfile
import argparse

from qcore import utils

SUBMISSION_DIR_NAME = "submission_temp"
SUBMISSION_TAR = "submission.tar"
SUBMISSION_FILES = [
    "flist_*",
    "*_header.cfg",
    "machine_*.json",
    "submit.sh",
    "*.template",
    "*py",
    "*.pyc",
]

SUBMISSION_SL_LOGS = ["*.pbs", "*.sl", "*.err", "*.out"]

LF_DIR_NAME = "LF_temp"
LF_SUB_DIR_NAME = "OutBin"
LF_TAR = "LF.tar"
LF_FILES = ["Rlog", "Restart", "SlipOut"]


def tar_files(directory_to_tar, archive_name):
    """params: directory_to_tar:source dir of all files to tar
    archive_name: output dir for the tar.gz
    """
    if os.path.isfile(archive_name):
        open_type = "a"
        print("Adding files to existing tar")
    else:
        open_type = "w"
        print("Start making new tar")

    try:
        with tarfile.open(archive_name, open_type) as tar:
            tar.add(directory_to_tar, arcname=os.path.basename(directory_to_tar))
    except Exception as e:
        print("Failed to make tar with exception {}".format(e))
    else:
        print("Finished adding files to tar")


def move_files(sim_dir, dest_dir, file_patterns):
    """
    move all files that match any of the specified file patterns from sim dir to dest dir
    :param sim_dir: path to source realization folder, eg. /home/melody.zhu/Albury/Runs/Albury/Albury_HYP15-21_S1384
    :param dest_dir: path to destination dir
    :param file_patterns: a list of files/file_pattern to copy
    :return:
    """
    for f in file_patterns:
        for p in glob.glob1(sim_dir, f):
            try:
                shutil.move(os.path.join(sim_dir, p), os.path.join(dest_dir, p))
            except Exception as e:
                print(
                    "error while copy ing file from {} to {}\n{}".format(
                        sim_dir, dest_dir, e
                    )
                )


def create_temp_dirs(sim_dir, outer_dir_name, inner_dir_name=""):
    """
    creates two nested temp dirs containing files to be tared
    :param sim_dir: path to realization folder
    :param outer_dir_name: name of temporary dir for storing submission/lf related files to be tared
    :param inner_dir_name: name of sub_dir inside the temporary dir for storing submission/lf related files to be tared
    :return: paths to outer_dir and inner dir
    """
    outer_dir = os.path.join(sim_dir, outer_dir_name)
    utils.setup_dir(outer_dir)
    inner_dir = ""
    if inner_dir_name != "":
        inner_dir = os.path.join(sim_dir, outer_dir_name, inner_dir_name)
        utils.setup_dir(inner_dir)
    return outer_dir, inner_dir


def clean_up_submission_lf_files(
    sim_dir, submission_files_to_tar=[], lf_files_to_tar=[]
):
    """
    main function for moving, taring submission/lf files and deleting any temporary dirs created
    :param submission_files_to_tar: a list of additional submission related files to tar
    :param lf_files_to_tar: a list of additional lf related files to tar
    :return: creates submisson and lf tar.gz
    """
    submission_files_to_tar += SUBMISSION_FILES + SUBMISSION_SL_LOGS
    lf_files_to_tar += LF_FILES

    # create temporary submission dir
    submission_dir, _ = create_temp_dirs(sim_dir, SUBMISSION_DIR_NAME)

    # create temporary lf dir
    lf_dir, lf_sub_dir = create_temp_dirs(sim_dir, LF_DIR_NAME, LF_SUB_DIR_NAME)

    # move files to submission dir
    move_files(sim_dir, submission_dir, submission_files_to_tar)

    tar_files(submission_dir, os.path.join(sim_dir, SUBMISSION_TAR))

    # move files to lf dir
    move_files(os.path.join(sim_dir, "LF"), lf_dir, lf_files_to_tar)
    # copy e3d segments to lf sub dir
    e3d_segs_dir = os.path.join(sim_dir, "LF", "OutBin")
    for f in os.listdir(e3d_segs_dir):
        if "-" in f:  # e3d segments have '-' in the name
            shutil.move(os.path.join(e3d_segs_dir, f), os.path.join(lf_sub_dir, f))

    tar_files(lf_dir, os.path.join(sim_dir, LF_TAR))

    # remove temporary submission and lf dir
    shutil.rmtree(lf_dir)
    shutil.rmtree(submission_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "sim_dir",
        help="path to realization dir eg./home/melody.zhu/Albury/Runs/Albury/Albury_HYP15-21_S1384",
    )
    parser.add_argument(
        "-submission",
        "--submission_files_to_tar",
        nargs="+",
        default=[],
        help="Please specify additional submission related file(s)/file_pattern(with '*') to tar separated by a space(if more than one). Default is {}".format(
            " ".join(SUBMISSION_FILES + SUBMISSION_SL_LOGS)
        ),
    )
    parser.add_argument(
        "-lf",
        "--lf_files_to_tar",
        nargs="+",
        default=[],
        help="Please specify additional LF related file(s)/file_pattern(with '*')to tar separated by a space(if more than one). Default is {}".format(
            " ".join(LF_FILES)
        ),
    )
    args = parser.parse_args()

    clean_up_submission_lf_files(
        args.sim_dir,
        submission_files_to_tar=args.submission_files_to_tar,
        lf_files_to_tar=args.lf_files_to_tar,
    )
