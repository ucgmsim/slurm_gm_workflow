#!/usr/bin/env python
"""
Collects BB.bin files and reduces stations contained.
Outputs stored in one location.
Flac compress optional.
"""

from argparse import ArgumentParser
from glob import glob
import os
from shutil import copyfile
from subprocess import call

from mpi4py import MPI

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

COMM = MPI.COMM_WORLD
RANK = COMM.Get_rank()
SIZE = COMM.Get_size()
MASTER = 0
IS_MASTER = not RANK

# collect required arguements
args = None
if IS_MASTER:
    parser = ArgumentParser()
    arg = parser.add_argument
    arg("bb_pattern", help="glob pattern to find BB bin files")
    arg("out_dir", help="folder to place reduced binary files")
    arg("--ll", help="station file to reduce BB with")
    arg(
        "--noflac",
        help="do not compress results using flac method",
        action="store_true",
    )

    try:
        args = parser.parse_args()
    except SystemExit:
        # invalid arguments or -h
        COMM.Abort()

    if not os.path.isdir(args.out_dir):
        os.makedirs(args.out_dir)

args = COMM.bcast(args, root=MASTER)
# make sure order is always the same
bb_files = sorted(glob(args.bb_pattern))[RANK::SIZE]
if IS_MASTER:
    assert len(bb_files) > 0

for bb in bb_files:
    # XXX: sensitive to changes in naming scheme
    new_name = os.path.basename(
        os.path.abspath(os.path.join(bb, os.pardir, os.pardir, os.pardir))
    )
    print(new_name)
    if "_HYP" not in new_name:
        print("Naming scheme is different, check and change as required.")
        COMM.Abort()

    bb_new = os.path.join(args.out_dir, "{}.bin".format(new_name))
    bb_flac = os.path.join(args.out_dir, "{}.flac".format(new_name))
    if args.ll is not None:
        cmd = [os.path.join(SCRIPT_DIR, "bbbin2shrink.py"), bb, bb_new, args.ll]
        print(" ".join(cmd))
        call(cmd)
    elif args.noflac:
        copyfile(bb, bb_new)
    else:
        bb_new = bb

    if not args.noflac and os.path.isfile(bb_new):
        cmd = [os.path.join(SCRIPT_DIR, "bbbin2bbflac.py"), bb_new, bb_flac]
        print(" ".join(cmd))
        call(cmd)
