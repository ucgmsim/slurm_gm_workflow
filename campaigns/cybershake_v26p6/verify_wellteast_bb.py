"""Read-only verification of the WellTeast canonical BB outputs.

Per target with a BB.bin:
  1. The file is exactly the canonical size, its header names are the
     canonical list in order, and every station is checkpointed (vsite > 0).
  2. Every station record matches its sources by name: lon/lat/x/y/z the LF
     record, e_dist and hf_vs_ref the HF record, vsite the vs30 file. This is
     what shows lf_idx and hf_idx picked the right stations.
  3. Waveform rows sit under the right names. For sampled stations, including
     both sides of where 320077e was, the low-passed BB trace correlates ~1
     with the same station's LF and far less with the LF of the station
     farthest from it.
  4. The six re-runs only: every station record, and the sampled waveforms,
     are compared with the preserved 18432-station original
     (BB.bin.with_320077e). The same code, venv and flags should reproduce
     them bit for bit.
Across targets, the per-station columns that do not depend on the rupture
(lon, lat, x, y, z, lf_vs_ref, vsite) must be identical.

Usage: python verify_wellteast_bb.py LIST TARGETS [N_RANDOM]
"""

import sys
from pathlib import Path

import numpy as np
from qcore import timeseries

HEAD_SIZE, HEAD_STAT = 0x500, 0x2C
NT = 59848  # samples per component in every WellTeast BB output (dt 0.005)
DROPPED = "320077e"
VS30 = "/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.vs30"
STATION_DTYPE = np.dtype([
    ("lon", "<f4"), ("lat", "<f4"), ("name", "|S8"), ("x", "<i4"), ("y", "<i4"), ("z", "<i4"),
    ("e_dist", "<f4"), ("hf_vs_ref", "<f4"), ("lf_vs_ref", "<f4"), ("vsite", "<f4"),
])
SITE_COLUMNS = ["lon", "lat", "x", "y", "z", "lf_vs_ref", "vsite"]


def read_bb(path: Path):
    """Header values, station records and a waveform memmap, straight from the file."""
    with open(path, "rb") as f:
        nstat, nt = (int(v) for v in np.fromfile(f, dtype="<i4", count=2))
        duration, dt, start = np.fromfile(f, dtype="<f4", count=3)
        f.seek(HEAD_SIZE)
        rec = np.fromfile(f, dtype=STATION_DTYPE, count=nstat)
    data = np.memmap(path, dtype="<f4", mode="r", offset=HEAD_SIZE + nstat * HEAD_STAT, shape=(nstat, nt, 3))
    names = [n.decode() for n in rec["name"]]
    return {"nstat": nstat, "nt": nt, "dt": float(dt), "start": float(start), "duration": float(duration),
            "rec": rec, "data": data, "names": names}


def first_positions(names) -> dict[str, int]:
    """Name -> first index, skipping blanks: the convention bb_sim uses."""
    pos: dict[str, int] = {}
    for i, raw in enumerate(names):
        name = str(raw)
        if name and name not in pos:
            pos[name] = i
    return pos


def lowpass(x, dt):
    return np.stack([timeseries.bwfilter(x[:, c], dt, 0.5, "lowpass") for c in range(3)], axis=1)


def corr(a, b):
    return min(np.corrcoef(a[:, c], b[:, c])[0, 1] for c in range(3))


def verify(rel: Path, canon: list[str], vs30: dict, sample: list[int], drop_pos: int):
    problems, notes = [], []
    path = rel / "BB" / "Acc" / "BB.bin"
    size, expected = path.stat().st_size, HEAD_SIZE + len(canon) * (HEAD_STAT + NT * 3 * 4)
    if size != expected:
        problems.append(f"size {size} != {expected}")
        return problems, notes, None
    bb = read_bb(path)
    rec = bb["rec"]
    if bb["names"] != canon:
        problems.append("header names are not the canonical list in order")
        return problems, notes, None
    n_unckpt = int((rec["vsite"] <= 0).sum())
    if n_unckpt:
        problems.append(f"{n_unckpt} station(s) without a checkpoint")

    # 2. each record against its sources, by name
    lf = timeseries.LFSeis(str(rel / "LF" / "OutBin"))
    hf = timeseries.HFSeis(str(rel / "HF" / "Acc" / "HF.bin"))
    lf_pos, hf_pos = first_positions(lf.stations.name), first_positions(hf.stations.name)
    lf_sel = np.array([lf_pos[n] for n in canon])
    hf_sel = np.array([hf_pos[n] for n in canon])
    for col in ["lon", "lat", "x", "y", "z"]:
        if not np.array_equal(rec[col], lf.stations[col][lf_sel]):
            problems.append(f"{col} differs from the LF record of the same name")
    if not np.array_equal(rec["e_dist"], hf.stations.e_dist[hf_sel]):
        problems.append("e_dist differs from the HF record of the same name")
    if not np.array_equal(rec["hf_vs_ref"], hf.stations.vs[hf_sel]):
        problems.append("hf_vs_ref differs from the HF record of the same name")
    if not np.array_equal(rec["vsite"], np.array([vs30[n] for n in canon], dtype="f4")):
        problems.append("vsite differs from the vs30 file")

    # 3. waveform rows under the right names
    lat, lon = np.radians(rec["lat"].astype(float)), np.radians(rec["lon"].astype(float))
    worst, best_ctrl = 1.0, -1.0
    for k in sample:
        d = np.arccos(np.clip(np.sin(lat[k]) * np.sin(lat) + np.cos(lat[k]) * np.cos(lat) * np.cos(lon - lon[k]), -1, 1))
        other = canon[int(np.argmax(d))]
        b = lowpass(np.asarray(bb["data"][k], dtype=float), bb["dt"])
        worst = min(worst, corr(b, lowpass(lf.acc(canon[k], dt=bb["dt"])[: bb["nt"]], bb["dt"])))
        best_ctrl = max(best_ctrl, corr(b, lowpass(lf.acc(other, dt=bb["dt"])[: bb["nt"]], bb["dt"])))
    notes.append(f"LF match r>={worst:.4f} control r<={best_ctrl:.4f}")
    if not (worst > 0.99 and best_ctrl < 0.5):
        problems.append("waveform alignment suspect")

    # 4. the six re-runs against their preserved originals
    orig_path = rel / "BB" / "Acc" / "BB.bin.with_320077e"
    if orig_path.exists():
        orig = read_bb(orig_path)
        opos = {n: i for i, n in enumerate(orig["names"])}
        osel = np.array([opos[n] for n in canon])
        for key in ("nt", "dt", "start", "duration"):
            if orig[key] != bb[key]:
                problems.append(f"header {key} {bb[key]} != original {orig[key]}")
        differing = [c for c in STATION_DTYPE.names if not np.array_equal(rec[c], orig["rec"][c][osel])]
        if differing:
            problems.append(f"station records differ from the original in {differing}")
        max_diff = max(float(np.abs(np.asarray(bb["data"][k], dtype=float) - orig["data"][osel[k]]).max())
                       for k in sample)
        n_equal = sum(np.array_equal(bb["data"][k], orig["data"][osel[k]]) for k in sample)
        notes.append(f"vs original: records {'identical' if not differing else 'DIFFER'}, "
                     f"waveforms identical {n_equal}/{len(sample)} (max |diff| {max_diff:.3g})")
        if n_equal != len(sample):
            problems.append("sampled waveforms not identical to the original")

    return problems, notes, rec


def main():
    canon = [n for n in (ln.split("#", 1)[0].strip() for ln in Path(sys.argv[1]).read_text().splitlines()) if n]
    targets = [Path(p) for p in Path(sys.argv[2]).read_text().split()]
    n_random = int(sys.argv[3]) if len(sys.argv) > 3 else 12

    vs30_arr = np.loadtxt(VS30, dtype=[("name", "|U8"), ("vs30", "f4")], comments=("#", "%"))
    vs30 = dict(zip(vs30_arr["name"], vs30_arr["vs30"]))

    # where 320077e sat in the 18432-station order: rows from here on shifted
    hf0 = timeseries.HFSeis(str(targets[0] / "HF" / "Acc" / "HF.bin"))
    drop_pos = [str(n) for n in hf0.stations.name].index(DROPPED)
    rng = np.random.default_rng(0)
    sample = sorted({0, drop_pos - 1, drop_pos, drop_pos + 1, len(canon) // 2, len(canon) - 1}
                    | set(int(i) for i in rng.choice(len(canon), n_random, replace=False)))
    print(f"canonical list: {len(canon)}; {DROPPED} was at index {drop_pos}; sampled rows: {sample}")

    site_ref, n_fail, n_missing = None, 0, 0
    for rel in targets:
        if not (rel / "BB" / "Acc" / "BB.bin").exists():
            print(f"{rel.name:<16} NOT YET: no BB.bin", flush=True)
            n_missing += 1
            continue
        problems, notes, rec = verify(rel, canon, vs30, sample, drop_pos)
        if rec is not None:
            if site_ref is None:
                site_ref = (rel.name, rec)
            else:
                differ = [c for c in SITE_COLUMNS if not np.array_equal(rec[c], site_ref[1][c])]
                if differ:
                    problems.append(f"site columns {differ} differ from {site_ref[0]}")
        n_fail += bool(problems)
        status = "OK" if not problems else "FAIL: " + "; ".join(problems)
        print(f"{rel.name:<16} {status} | {'; '.join(notes)}", flush=True)

    n_ok = len(targets) - n_fail - n_missing
    print(f"\n{n_ok} verified, {n_fail} failed, {n_missing} without output, of {len(targets)} targets")
    sys.exit(1 if n_fail or n_missing else 0)


if __name__ == "__main__":
    main()
