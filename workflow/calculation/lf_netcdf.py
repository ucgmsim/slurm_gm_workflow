"""Read an LF NetCDF from the new workflow as bb_sim reads an EMOD3D OutBin.

The new workflow's `lf-to-xarray` stores an EMOD3D run's LF output as a NetCDF
file. In the version that converted the Cybershake v26p5 HikWgtnmax LF (workflow
`pegasus` at 7e465c5, which calls qcore-utils 2025.12.2
`timeseries.read_lfseis_directory`), each station's three velocity components are
rotated to 090, 000 and ver, then differentiated with `np.gradient`: central
differences inside the record and one-sided differences at its two ends. The
result is stored as float32 acceleration in cm/s^2.

bb_sim instead expects `qcore.timeseries.LFSeis`. Its `acc` differentiates the
rotated velocity with a backward difference from rest (`vel2acc3d`). So
`LFNetCDF.vel` rebuilds the velocity from the stored central differences,
starting from rest, as two interleaved running sums in float64, and `acc`
applies `vel2acc3d` to it. bb_sim thus gets what LFSeis would give it from the
same OutBin, up to float32 rounding. Central differences cannot see a component
alternating at the Nyquist frequency. EMOD3D output has none, and any rounding
residue there is removed by bb_sim's lowpass at flo.

Metadata follow LFSeis:
  - dt, hh (resolution) and rot come from the file as float32, and nt from its
    time axis. All are checked against the run's e3d.par, so a NetCDF and an
    e3d.par from different runs are refused.
  - start_sec is -1/flo, times 3 for EMOD3D versions after 3.0.4, with flo and
    version from e3d.par. It is checked against the file's start_sec attribute.
    The new workflow's e3d.par has comment and blank lines that qcore's
    load_e3d_par cannot parse, so read_e3d_par reads it instead, giving the
    same raw values.
  - The stations' name, x, y, lat and lon come from the file. z comes from the
    station coordinates file EMOD3D ran with, which must agree on x and y,
    because the NetCDF does not carry it. Stations are ordered as in that file,
    as LFSeis orders them.
  - A station stored more than once (EMOD3D boundary duplicates) keeps its last
    copy, as LFSeis keeps the last seis file's.
"""

from pathlib import Path

import h5py
import numpy as np
from qcore.constants import MAXIMUM_EMOD3D_TIMESHIFT_1_VERSION
from qcore.timeseries import vel2acc3d
from qcore.utils import compare_versions

STATION_DTYPE = [("x", "i4"), ("y", "i4"), ("z", "i4"), ("lat", "f4"), ("lon", "f4"), ("name", "U7")]
E3D_PAR_KEYS = ("version", "flo", "nt", "dt", "h", "modelrot")  # what LFNetCDF reads


def is_lf_netcdf(path) -> bool:
    """Whether bb_sim's lf_dir argument names an LF NetCDF rather than an OutBin."""
    return Path(path).suffix == ".nc" and Path(path).is_file()


def rebuild_velocity(acc: np.ndarray, dt: float) -> np.ndarray:
    """The velocity from rest whose `np.gradient(v, dt, axis=0)` is `acc`.

    np.gradient gives acc[0] = (v[1] - v[0]) / dt and, inside the record,
    acc[i] = (v[i+1] - v[i-1]) / (2 dt). With v[0] = 0 these are solved exactly,
    even and odd samples separately. The last sample, a one-sided difference,
    is not needed. dt must be the step np.gradient was given.
    """
    acc = np.asarray(acc, dtype=np.float64)
    n = acc.shape[0]
    v = np.zeros_like(acc)
    if n > 1:
        v[1] = dt * acc[0]
    if n > 2:
        v[2::2] = 2 * dt * np.cumsum(acc[1 : n - 1 : 2], axis=0)
        v[3::2] = v[1] + 2 * dt * np.cumsum(acc[2 : n - 1 : 2], axis=0)
    return v


def read_e3d_par(path) -> dict[str, str]:
    """Key -> raw value (as qcore's load_e3d_par gives it) for each "key=value" line.

    Blank lines and "#" comments are skipped. The new workflow's e3d.par has both,
    and load_e3d_par fails on them. Its run-specific overrides can repeat a
    default, so a key given twice keeps its last value. The keys LFNetCDF reads
    must appear only once.
    """
    pars, seen = {}, set()
    for line in Path(path).read_text().splitlines(keepends=True):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            raise ValueError(f"{path}: line without '=': {stripped!r}")
        if key in seen and key in E3D_PAR_KEYS:
            raise ValueError(f"{path}: {key} given more than once")
        seen.add(key)
        pars[key] = value
    return pars


def par_number(pars, key) -> str:
    """A numeric e3d.par value without its quotes: the old workflow writes nt="59107"."""
    return pars[key].strip().strip('"')


def read_statcords(path) -> dict[str, tuple[int, int, int, int]]:
    """Station name -> (line position, x, y, z) from an EMOD3D station coordinates file.

    The first line holds the station count, and each following line "x y z name".
    """
    lines = Path(path).read_text().split("\n")
    count = int(lines[0].split()[0])
    coords = {}
    for position, line in enumerate(ln for ln in lines[1:] if ln.strip()):
        x, y, z, name = line.split()[:4]
        if name in coords:
            raise ValueError(f"{path}: station {name} listed twice")
        coords[name] = (position, int(x), int(y), int(z))
    if len(coords) != count:
        raise ValueError(f"{path}: header says {count} stations, found {len(coords)}")
    return coords


class LFNetCDF:
    """The part of qcore.timeseries.LFSeis that bb_sim uses, read from an LF NetCDF.

    netcdf: the NetCDF lf-to-xarray wrote for one realisation.
    e3d_par: that realisation's EMOD3D parameter file.
    statcords: the station coordinates file EMOD3D ran with (its seiscords).
    """

    def __init__(self, netcdf, e3d_par, statcords):
        self.path = str(netcdf)
        self._file = h5py.File(self.path, "r")
        self._waveform = self._file["waveform"]
        attrs = self._file.attrs
        if attrs["units"] != "cm/s^2" or self._waveform.shape[0] != 3:
            raise ValueError(f"{self.path}: expected (3, station, time) acceleration in cm/s^2")

        # the step np.gradient was given, as stored (a float64 of the float32 step)
        self._gradient_dt = float(np.asarray(attrs["dt"]).squeeze())
        self.dt = np.float32(self._gradient_dt)
        self.hh = np.float32(np.asarray(attrs["resolution"]).squeeze())
        self.rot = np.float32(np.asarray(attrs["rotation"]).squeeze())
        self.nt = np.int32(self._waveform.shape[2])
        self.duration = self.nt * self.dt

        pars = read_e3d_par(e3d_par)
        self.flo = float(par_number(pars, "flo"))
        self.emod3d_version = pars["version"]
        self.start_sec = -1 / self.flo
        if compare_versions(self.emod3d_version, MAXIMUM_EMOD3D_TIMESHIFT_1_VERSION) > 0:
            self.start_sec *= 3
        stored_start = float(np.asarray(attrs["start_sec"]).squeeze())
        checks = {
            "nt": (int(par_number(pars, "nt")), int(self.nt)),
            "dt": (np.float32(float(par_number(pars, "dt"))), self.dt),
            "h": (np.float32(float(par_number(pars, "h"))), self.hh),
        }
        for key, (from_par, from_file) in checks.items():
            if from_par != from_file:
                raise ValueError(f"{self.path}: {key} {from_file} differs from {e3d_par} ({from_par})")
        if not np.isclose(np.float32(float(par_number(pars, "modelrot"))), self.rot, atol=1e-4):
            raise ValueError(f"{self.path}: rotation {self.rot} differs from {e3d_par} ({pars['modelrot']})")
        if not np.isclose(stored_start, self.start_sec, atol=1e-5):
            raise ValueError(f"{self.path}: start_sec {stored_start} differs from e3d.par's {self.start_sec}")

        names = [n.decode() if isinstance(n, bytes) else str(n) for n in self._file["station"][:]]
        last = {name: i for i, name in enumerate(names)}  # a duplicate keeps its last copy
        coords = read_statcords(statcords)
        unknown = sorted(set(last) - set(coords))
        if unknown:
            raise ValueError(f"{self.path}: {len(unknown)} stations not in {statcords}, e.g. {unknown[:3]}")
        order = sorted(last, key=lambda name: coords[name][0])
        self._row = np.array([last[name] for name in order], dtype=np.int64)

        stations = np.zeros(len(order), dtype=STATION_DTYPE)
        stations["name"] = order
        for col in ("x", "y", "lat", "lon"):
            stations[col] = self._file[col][:][self._row]
        stations["z"] = [coords[name][3] for name in order]
        for col, j in (("x", 1), ("y", 2)):
            expected = np.array([coords[name][j] for name in order])
            if not np.array_equal(stations[col], expected):
                raise ValueError(f"{self.path}: station {col} differs from {statcords}")
        self.stations = stations.view(np.recarray)
        self.nstat = self.stations.size
        self.stat_idx = dict(zip(self.stations.name, np.arange(self.nstat)))
        self.n_duplicate = len(names) - len(last)

    def vel(self, station, dt=None):
        """Velocity (nt, 3) in cm/s for 090, 000, ver, as LFSeis.vel returns it."""
        row = self._row[self.stat_idx[station]]
        acc = self._waveform[:, row, :].T  # (nt, 3)
        velocity = rebuild_velocity(acc, self._gradient_dt)
        if dt is None or dt == self.dt:
            return velocity
        raise NotImplementedError(f"resampling LF from {self.dt} to {dt} is not supported")

    def acc(self, station, dt=None):
        """Acceleration (nt, 3) in cm/s^2, as LFSeis.acc returns it."""
        if dt is None:
            dt = self.dt
        return vel2acc3d(self.vel(station, dt=dt), dt)
