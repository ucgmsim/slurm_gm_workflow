"""LFNetCDF must give bb_sim what LFSeis gives it from the same EMOD3D OutBin.

The end-to-end test writes a small synthetic OutBin in EMOD3D's LFSeis format and
converts it exactly as the Cybershake v26p5 HikWgtnmax LF was converted:
qcore-utils 2025.12.2 `read_lfseis_directory`, then `to_netcdf(engine="h5netcdf")`,
which is all `lf-to-xarray` did at workflow `pegasus` 7e465c5. It then compares
LFNetCDF with LFSeis reading the OutBin directly. Tests that need that qcore,
xarray or h5netcdf are skipped where they are missing.
"""

from pathlib import Path

import numpy as np
import pytest

h5py = pytest.importorskip("h5py")
timeseries = pytest.importorskip("qcore.timeseries")
from workflow.calculation.lf_netcdf import LFNetCDF, rebuild_velocity  # noqa: E402

BB_SIM = Path(__file__).resolve().parents[1] / "bb_sim.py"
DT = np.float32(0.005)
NT = 1201
ROT = np.float32(19.776508)  # PalliserKai's model rotation
HH = np.float32(0.1)


@pytest.mark.parametrize("n", [2, 3, 10, 11, 1201])
def test_rebuild_velocity_inverts_np_gradient(n):
    rng = np.random.default_rng(n)
    v = np.cumsum(rng.normal(size=(n, 3)), axis=0)
    v[0] = 0
    back = rebuild_velocity(np.gradient(v, 0.005, axis=0), 0.005)
    np.testing.assert_allclose(back[1:], v[1:], rtol=0, atol=1e-9 * np.abs(v).max())
    assert np.all(back[0] == 0)


def _write_outbin(root: Path, files: list[list[int]], stations: list[dict], velocity: dict) -> Path:
    """An EMOD3D OutBin: each seis file holds the stations whose global indices it lists."""
    outbin = root / "LF" / "OutBin"
    outbin.mkdir(parents=True)
    head = np.dtype({"names": ["stat_pos", "x", "y", "z", "nt", "dt", "hh", "rot", "lat", "lon", "name"],
                     "formats": ["<i4", "<i4", "<i4", "<i4", "<i4", "<f4", "<f4", "<f4", "<f4", "<f4", "S8"],
                     "offsets": [0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40], "itemsize": 48})
    for number, members in enumerate(files):
        records = np.zeros(len(members), dtype=head)
        data = np.zeros((NT, len(members), 9), dtype="<f4")
        for j, (pos, copy) in enumerate(members):
            s = stations[pos]
            records[j] = (pos, s["x"], s["y"], 1, NT, DT, HH, ROT, s["lat"], s["lon"], s["name"].encode())
            data[:, j, :3] = velocity[(pos, copy)]
            data[:, j, 3:] = 1e6  # EMOD3D's other six components: must be ignored
        with open(outbin / f"Test_seis-{number:05d}.e3d", "wb") as f:
            np.array([len(members)], dtype="<i4").tofile(f)
            records.tofile(f)
            data.tofile(f)
    return outbin


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    """A synthetic run with a duplicate station, converted to NetCDF like HikWgtnmax."""
    xr = pytest.importorskip("xarray")
    pytest.importorskip("h5netcdf")
    if not hasattr(timeseries, "read_lfseis_directory"):
        pytest.skip("needs the qcore that converted the Cascade LF (qcore-utils 2025.12.2)")
    root = tmp_path_factory.mktemp("lf")
    rng = np.random.default_rng(7)
    n = 12
    stations = [{"name": f"S{i:03d}" if i % 3 else f"ST{i:05d}", "x": 100 + 7 * i, "y": 300 - 5 * i,
                 "lat": np.float32(-41 - 0.01 * i), "lon": np.float32(174 + 0.02 * i)} for i in range(n)]
    t = np.arange(NT) * float(DT)
    velocity = {}
    for pos in range(n):
        # quiet for 3 s, then a band-limited wave train, as EMOD3D output starts at rest
        w = np.zeros((NT, 3))
        for c in range(3):
            for f, a, p in zip(rng.uniform(0.1, 2.0, 6), rng.uniform(1, 20, 6), rng.uniform(0, 6, 6)):
                w[:, c] += a * np.sin(2 * np.pi * f * (t - 3) + p) * np.exp(-0.5 * (t - 4.5) ** 2)
        w[t < 3] = 0
        velocity[(pos, 0)] = w.astype("<f4")
    # station 5 sits on an MPI boundary: written by files 0 and 2, slightly differently
    velocity[(5, 1)] = (velocity[(5, 0)] * 1.01).astype("<f4")
    files = [[(0, 0), (1, 0), (2, 0), (5, 0)], [(3, 0), (4, 0), (6, 0), (7, 0)],
             [(5, 1), (8, 0), (9, 0), (10, 0), (11, 0)]]
    outbin = _write_outbin(root, files, stations, velocity)
    # LFSeis's own parser takes only plain "key=value" lines ...
    plain = f'version="3.0.8-mpi"\nflo=1.0\nnt={NT}\ndt=0.005\nh=0.1\nmodelrot={float(ROT)}\n'
    (root / "LF" / "e3d.par").write_text(plain)
    # ... while the new workflow's (HikWgtnmax's) has comments, blank lines and
    # defaults that run-specific overrides repeat; LFNetCDF gets that kind
    e3d_par = root / "e3d_cascade.par"
    e3d_par.write_text("# --- Defaults from emod3d_defaults.yaml ---\ndump_itinc=4000\n\n"
                       "# --- Run-Specific Overrides ---\n" + plain + "\ndump_itinc=1201\n")
    statcords = root / "stations.statcords"
    statcords.write_text(f"{n}\n" + "".join(f"{s['x']} {s['y']} 1 {s['name']}\n" for s in stations))
    nc = root / "lf.nc"
    timeseries.read_lfseis_directory(outbin).to_netcdf(nc, engine="h5netcdf")
    return {"outbin": outbin, "nc": nc, "e3d_par": e3d_par, "statcords": statcords,
            "names": [s["name"] for s in stations], "velocity": velocity}


def test_metadata_matches_lfseis(run):
    ref = timeseries.LFSeis(str(run["outbin"]))
    lf = LFNetCDF(run["nc"], run["e3d_par"], run["statcords"])
    for key in ("nt", "dt", "hh", "rot", "duration", "start_sec"):
        assert getattr(lf, key) == getattr(ref, key), key
        assert type(getattr(lf, key)) is type(getattr(ref, key)), key
    assert list(lf.stations.name) == list(ref.stations.name) == run["names"]
    for col in ("x", "y", "z", "lat", "lon"):
        np.testing.assert_array_equal(lf.stations[col], ref.stations[col], err_msg=col)
    assert lf.n_duplicate == 1


def test_acceleration_matches_lfseis_through_bb_lowpass(run):
    ref = timeseries.LFSeis(str(run["outbin"]))
    lf = LFNetCDF(run["nc"], run["e3d_par"], run["statcords"])
    for name in run["names"]:
        a, b = lf.acc(name, dt=0.005), ref.acc(name, dt=0.005)
        assert a.shape == b.shape == (NT, 3)
        peak = np.abs(b).max()
        # raw: float32 rounding, plus a Nyquist residue central differences cannot see
        assert np.abs(a - b).max() <= 1e-4 * peak, name
        # what bb_sim keeps: LF lowpassed at flo = 1 Hz
        la = np.stack([timeseries.bwfilter(a[:, c], 0.005, 1.0, "lowpass") for c in range(3)], axis=1)
        lb = np.stack([timeseries.bwfilter(b[:, c], 0.005, 1.0, "lowpass") for c in range(3)], axis=1)
        assert np.abs(la - lb).max() <= 1e-5 * np.abs(lb).max(), name


def test_duplicate_keeps_the_last_copy_like_lfseis(run):
    ref = timeseries.LFSeis(str(run["outbin"]))
    lf = LFNetCDF(run["nc"], run["e3d_par"], run["statcords"])
    name = run["names"][5]
    np.testing.assert_allclose(lf.vel(name), ref.vel(name), rtol=0, atol=1e-4 * np.abs(ref.vel(name)).max())
    # and that copy is the second one (file 2), 1.01 times the first
    first = run["velocity"][(5, 0)].astype(np.float64) @ ref.rot_matrix
    assert np.isclose(np.abs(lf.vel(name)).max() / np.abs(first).max(), 1.01, rtol=1e-4)


def test_accepts_the_old_workflows_quoted_numbers(run, tmp_path):
    # the old workflow's e3d.par (e.g. PalliserKai's) writes nt="59107"
    quoted = tmp_path / "e3d.par"
    quoted.write_text(run["e3d_par"].read_text().replace(f"nt={NT}", f'nt="{NT}"'))
    assert LFNetCDF(run["nc"], quoted, run["statcords"]).nt == NT


def test_refuses_a_key_it_reads_given_twice(run, tmp_path):
    bad = tmp_path / "e3d.par"
    bad.write_text(run["e3d_par"].read_text() + "flo=1.0\n")
    with pytest.raises(ValueError, match="flo given more than once"):
        LFNetCDF(run["nc"], bad, run["statcords"])


@pytest.mark.parametrize("change,message", [
    (("nt=1201", "nt=1200"), "nt"),
    (("h=0.1", "h=0.2"), "h"),
    (("modelrot=", "modelrot=1"), "rotation"),
    (("flo=1.0", "flo=0.5"), "start_sec"),
])
def test_refuses_an_e3d_par_from_another_run(run, tmp_path, change, message):
    bad = tmp_path / "e3d.par"
    bad.write_text(run["e3d_par"].read_text().replace(*change))
    with pytest.raises(ValueError, match=message):
        LFNetCDF(run["nc"], bad, run["statcords"])


def test_refuses_station_coordinates_that_disagree(run, tmp_path):
    lines = run["statcords"].read_text().splitlines()
    x, y, z, name = lines[3].split()
    lines[3] = f"{int(x) + 1} {y} {z} {name}"
    bad = tmp_path / "bad.statcords"
    bad.write_text("\n".join(lines) + "\n")
    with pytest.raises(ValueError, match="station x"):
        LFNetCDF(run["nc"], run["e3d_par"], bad)


def test_bb_sim_uses_the_reader_for_netcdf_and_zeroes_vsite():
    source = BB_SIM.read_text()
    assert "LFNetCDF(args.lf_dir, args.lf_e3d_par, args.lf_statcords)" in source
    assert '"--lf-e3d-par"' in source and '"--lf-statcords"' in source
    # the station records must come from np.zeros itself, not a field-by-field copy
    assert "bb_stations = np.rec.array(" not in source
    assert ").view(np.recarray)" in source
