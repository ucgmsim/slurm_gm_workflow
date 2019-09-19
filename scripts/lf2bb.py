"""Converts the contents of an outbin directory to a BB binary at a given location."""
import os
import argparse
import numpy as np
from qcore.timeseries import BBSeis, LFSeis


def lf2bb(outbin, vs30file, bb_file, dt=None):
    """Converts the contents of the outbin folder to a BB binary at the given location.
    Writes the header using any available information, the parts provided by HF are blank however.
    :param outbin: The location of the outbin directory
    :param bb_file: The location the BB binary is to be written to
    """
    FLOAT_SIZE = 0x4
    N_COMP = BBSeis.N_COMP
    HEAD_SIZE = BBSeis.HEAD_SIZE
    HEAD_STAT = BBSeis.HEAD_STAT

    lf_data = LFSeis(outbin)

    if dt is None:
        dt = lf_data.dt

    if dt != lf_data.dt:
        nt = int(lf_data.duration/dt)+1
    else:
        nt = lf_data.nt

    head_total = HEAD_SIZE + lf_data.stations.size * HEAD_STAT
    file_size = head_total + lf_data.stations.size * nt * N_COMP * FLOAT_SIZE

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
                    "vsite",
                ],
                "formats": [
                    "f4",
                    "f4",
                    "|S8",
                    "i4",
                    "i4",
                    "i4",
                    "f4",
                    "f4",
                    "f4",
                    "f4",
                ],
                "itemsize": HEAD_STAT,
            },
        )
    )
    for col in bb_stations.dtype.names:
        if col in lf_data.stations.dtype.names:
            bb_stations[col] = lf_data.stations[col]

    bb_stations["vsite"] = np.vectorize(
        dict(
            np.loadtxt(
                vs30file, dtype=[("name", "|U8"), ("vs30", "f4")], comments=("#", "%")
            )
        ).get
    )(lf_data.stations.name)

    with open(bb_file, "wb") as out:
        # Write the header
        np.array([lf_data.stations.size, nt], dtype="i4").tofile(out)
        np.array([nt * dt, dt, -1], dtype="f4").tofile(out)
        np.array([os.path.abspath(outbin), "", ""], dtype="|S256").tofile(out)

        # Write the station information
        out.seek(HEAD_SIZE)
        bb_stations.tofile(out)

        # Write the last bite to ensure file size. Should be overwritten if everything works
        out.seek(file_size - FLOAT_SIZE)
        np.float32().tofile(out)

        # Write the acceleration data for each station in units of g (9.81m/s/s)
        out.seek(head_total)
        acc_dat = np.empty((nt, N_COMP), dtype="f4")
        for i, stat in enumerate(lf_data.stations):
            acc_dat[:] = (lf_data.acc(stat.name, dt=dt) / 981)[:]
            acc_dat.tofile(out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("outbin_dir_loc", help="Location of the outbin directory")
    parser.add_argument("vsite_file", help="Vs30 station file")
    parser.add_argument("bb_bin_loc", help="Location to save the bb binary to")
    parser.add_argument(
        "--dt", help="Change the dt of the LF simulation", default=None, type=float
    )
    args = parser.parse_args()
    lf2bb(args.outbin_dir_loc, args.vsite_file, args.bb_bin_loc, args.dt)


if __name__ == "__main__":
    main()
