import argparse
from os.path import abspath
from sys import stderr

from qcore.xyts import XYTSFile


def check_xyts_file(file_path: str):
    """Opens the given file and attempts to extract information from it"""
    xyts_file = XYTSFile(file_path)
    corners, gmt_corners = xyts_file.corners(True)
    xyts_file.region(corners)
    xyts_file.tslice_get(0)
    xyts_file.pgv(True)
    return True


def check_zero_bytes(file_path: str):
    """Checks that all PGV values are above zero"""
    xyts_file = XYTSFile(file_path)
    pgv = xyts_file.pgv()[:, 2]
    return min(pgv) > 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("xyts_file", type=str, help="The merged xyts file to test")
    args = parser.parse_args()
    file_path = abspath(args.xyts_file)
    try:
        if check_xyts_file(file_path) and check_zero_bytes(file_path):
            return True
    except Exception as e:
        print(e, file=stderr)
        print(
            "Attempt to open and extract information from xyts file {} failed".format(
                file_path
            )
        )
        return False


if __name__ == "__main__":
    if main():
        exit(0)
    else:
        exit(1)
