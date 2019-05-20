import os
import argparse
import numpy as np
from qcore.timeseries import BBSeis, LFSeis


def lf2bb(outbin, bb_file):
    FLOAT_SIZE = 0x4
    N_COMP = BBSeis.N_COMP
    HEAD_SIZE = BBSeis.HEAD_SIZE
    HEAD_STAT = BBSeis.HEAD_STAT

    lf_data = LFSeis(outbin)

    head_total = HEAD_SIZE + lf_data.stations.size * HEAD_STAT
    file_size = head_total + lf_data.stations.size * lf_data.nt * N_COMP * FLOAT_SIZE

    bb_stations = np.rec.array(
        np.zeros(
            lf_data.nstat,
            dtype={
                "names": [
                    "lon",
                    "lat",
                    "name",
                    "x",
                    "y",
                    "z",
                    "e_dist",
                    "hf_vs_ref",
                    "lf_vs_ref",
                ],
                "formats": ["f4", "f4", "|S8", "i4", "i4", "i4", "f4", "f4", "f4"],
                "itemsize": HEAD_STAT,
            },
        )
    )
    for col in bb_stations.dtype.names[:-3]:
        bb_stations[col] = lf_data.stations[col]

    with open(bb_file, 'wb') as out:
        np.array([lf_data.stations.size, lf_data.nt], dtype="i4").tofile(out)
        np.array([lf_data.nt * lf_data.dt, lf_data.dt, -1], dtype="f4").tofile(out)
        np.array([os.path.abspath(outbin), "", ""], dtype="|S256").tofile(out)
        out.seek(HEAD_SIZE)
        bb_stations.tofile(out)
        out.seek(file_size - FLOAT_SIZE)
        np.float32().tofile(out)
        out.seek(head_total)
        acc_dat = np.empty((lf_data.nt, N_COMP), dtype="f4")
        for i, stat in enumerate(lf_data.stations):
            acc_dat[:] = (lf_data.acc(stat.name) / 981)[:]
            acc_dat.tofile(out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("outbin_dir_loc", help="Location of the outbin directory")
    parser.add_argument("bb_bin_loc", help="Location to save the bb binary to")
    args = parser.parse_args()
    lf2bb(args.outbin_dir_loc, args.bb_bin_loc)


if __name__ == '__main__':
    main()
