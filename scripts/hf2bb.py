import argparse
import numpy as np
from qcore.timeseries import BBSeis, HFSeis


def hf2bb(hf_bin, bb_bin):
    FLOAT_SIZE = 0x4
    N_COMP = BBSeis.N_COMP
    HEAD_SIZE = BBSeis.HEAD_SIZE
    HEAD_STAT = BBSeis.HEAD_STAT

    hf_data = HFSeis(hf_bin)

    head_total = HEAD_SIZE + hf_data.stations.size * HEAD_STAT
    file_size = head_total + hf_data.stations.size * hf_data.nt * N_COMP * FLOAT_SIZE

    bb_stations = np.rec.array(
        np.zeros(
            hf_data.nstat,
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
                    "vsite",
                ],
                "formats": ["f4", "f4", "|S8", "i4", "i4", "i4", "f4", "f4", "f4", "f4"],
                "itemsize": HEAD_STAT,
            },
        )
    )

    for col in bb_stations.dtype.names:
        if col in hf_data.stations.dtype.names:
            bb_stations[col] = hf_data.stations[col]

    bb_stations['vsite'] = hf_data.stations["vs"]

    with open(bb_bin, 'wb') as out:
        np.array([hf_data.stations.size, hf_data.nt], dtype="i4").tofile(out)
        np.array([hf_data.nt * hf_data.dt, hf_data.dt, hf_data.start_sec], dtype="f4").tofile(out)
        np.array(["", "", hf_bin], dtype="|S256").tofile(out)
        out.seek(HEAD_SIZE)
        bb_stations.tofile(out)
        out.seek(file_size - FLOAT_SIZE)
        np.float32().tofile(out)
        out.seek(head_total)
        acc_dat = np.empty((hf_data.nt, N_COMP), dtype="f4")
        for i, stat in enumerate(hf_data.stations):
            acc_dat[:] = (hf_data.acc(stat.name) / 981)[:]
            acc_dat.tofile(out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("hf_bin_loc", help="Location of the hf binary file")
    parser.add_argument("bb_bin_loc", help="Location to save the bb binary to")
    args = parser.parse_args()
    hf2bb(args.hf_bin_loc, args.bb_bin_loc)


if __name__ == '__main__':
    main()
