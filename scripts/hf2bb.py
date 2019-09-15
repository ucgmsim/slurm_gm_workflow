"""Converts the contents of an HF binary file to a BB binary file at a given location."""
import argparse
import numpy as np
from qcore.timeseries import BBSeis, HFSeis


def hf2bb(hf_bin, bb_bin, dt=None):
    """Converts a given HF binary file to a BB binary file at the given location.
    Writes the header using any available information, the parts provided by LF are blank however.
    :param hf_bin: The location of the HF binary
    :param bb_bin: The location the BB binary is to be written to
    """
    FLOAT_SIZE = 0x4
    N_COMP = BBSeis.N_COMP
    HEAD_SIZE = BBSeis.HEAD_SIZE
    HEAD_STAT = BBSeis.HEAD_STAT

    hf_data = HFSeis(hf_bin)

    if dt is None:
        dt = hf_data.dt

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
        if col in hf_data.stations.dtype.names:
            bb_stations[col] = hf_data.stations[col]

    bb_stations["vsite"] = hf_data.stations["vs"]

    with open(bb_bin, "wb") as out:
        # Write the header
        np.array([hf_data.stations.size, hf_data.nt], dtype="i4").tofile(out)
        np.array(
            [hf_data.nt * dt, dt, hf_data.start_sec], dtype="f4"
        ).tofile(out)
        np.array(["", "", hf_bin], dtype="|S256").tofile(out)

        # Write the station data
        out.seek(HEAD_SIZE)
        bb_stations.tofile(out)

        # Write the final byte to ensure the file size. Should be overwritten if everything works.
        out.seek(file_size - FLOAT_SIZE)
        np.float32().tofile(out)

        # Write the acceleration data for each station in units of g (9.81m/s/s)
        out.seek(head_total)
        acc_dat = np.empty((hf_data.nt, N_COMP), dtype="f4")
        for i, stat in enumerate(hf_data.stations):
            acc_dat[:] = (hf_data.acc(stat.name, dt=dt) / 981)[:]
            acc_dat.tofile(out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("hf_bin_loc", help="Location of the hf binary file")
    parser.add_argument("bb_bin_loc", help="Location to save the bb binary to")
    parser.add_argument("--dt", help="Change the dt of the HF simulation", default=None)
    args, extra = parser.parse_known_args()
    if len(extra) > 0:
        print("Not sure what to do with arguments \"{}\", ignoring".format(" ".join(extra)))
    hf2bb(args.hf_bin_loc, args.bb_bin_loc, args.dt)


if __name__ == "__main__":
    main()
