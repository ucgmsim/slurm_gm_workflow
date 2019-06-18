import argparse
from os.path import abspath, getsize
from sys import stderr

from qcore.xyts import XYTSFile


def test_xyts_file(file_path: str):
    """Opens the given file and attempts to extract information from it"""
    xyts_file = XYTSFile(file_path)
    corners, gmt_corners = xyts_file.corners(True)
    xyts_file.region(corners)
    xyts_file.tslice_get(0)
    xyts_file.pgv(True)
    return True


def check_zero_bytes(file_path: str):
    """Checks that there are a minimal number of zero bytes in the xyts file.
    Default value is 1 percent"""
    xyts_file = XYTSFile(file_path)
    time_slice = xyts_file.tslice_get(xyts_file.data.shape[0]//4)
    min_pvg = min(abs(time_slice[2, :]))
    return min_pvg > 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("xyts_file", type=str, help="The merged xyts file to test")
    args = parser.parse_args()
    file_path = abspath(args.xyts_file)
    try:
        if test_xyts_file(file_path) and check_zero_bytes(file_path):
            return True
    except Exception as e:
        print(e, file=stderr)
        print("Attempt to open and extract information from xyts file {} failed".format(file_path))
        return False


if __name__ == '__main__':
    if main():
        exit(0)
    else:
        exit(1)
