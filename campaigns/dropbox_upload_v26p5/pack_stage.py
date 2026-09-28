"""Pack one Cybershake stage's outputs into per-realisation tars for Dropbox, and verify them.

For each realisation directory in TARGETS it writes
STAGING/<STAGE>/<Fault>/<Rel>_<STAGE>.tar. The tar is uncompressed and PAX
format, and holds that stage's outputs under flat names prefixed with the
realisation, as the v25p10, v25p11 and v26p4 uploads do:
    HF: <Rel>_HF.bin, <Rel>_HF.log
    BB: <Rel>_BB.bin, <Rel>_BB.log
    IM: <Rel>.csv, <Rel>_imcalc.info, <Rel>_im_calc.log
(A median's <Rel> is the fault name, as in FiordSZ03_BB.tar.)

Before packing, each source is checked to be the finished output:
  - HF names the OneRay model and says "Simulation completed";
  - BB.bin has the size its header implies;
  - the IM CSV is not empty.
Each member's SHA-256 is computed from the source as it is written. The tar is
then re-read, and every member must give the same hash. The tar's own SHA-256
is recorded too.

A tar is written as .partial and renamed only once verified, so a complete tar
is never overwritten. A rerun skips tars already verified and rebuilds
.partial leftovers. Each tar's manifest rows go into STAGING/.manifest/, and
MANIFEST.tsv in the fault folder is rebuilt from them after every run.
Realisations with nothing for this stage (WellTeast_REL04 has no BB or IM) are
listed and skipped.

Usage: python pack_stage.py STAGE TARGETS STAGING
"""

import hashlib
import os
import sys
import tarfile
from pathlib import Path

import numpy as np

CHUNK = 64 << 20
HF_MODEL = b"Cant1D_v3-midQ_OneRay.1d"


class HashingReader:
    """A file wrapper that SHA-256s what tarfile reads through it."""

    def __init__(self, f):
        self.f, self.sha = f, hashlib.sha256()

    def read(self, n=-1):
        data = self.f.read(n)
        self.sha.update(data)
        return data


def sources(stage, rel):
    """[(source path, member name)] for this stage, or [] if the realisation has none."""
    name = rel.name
    if stage == "HF":
        pairs = [(rel / "HF" / "Acc" / "HF.bin", f"{name}_HF.bin"), (rel / "HF" / "Acc" / "HF.log", f"{name}_HF.log")]
    elif stage == "BB":
        pairs = [(rel / "BB" / "Acc" / "BB.bin", f"{name}_BB.bin"), (rel / "BB" / "Acc" / "BB.log", f"{name}_BB.log")]
    elif stage == "IM":
        pairs = [(rel / "IM_calc" / f"{name}.csv", f"{name}.csv"),
                 (rel / "IM_calc" / f"{name}_imcalc.info", f"{name}_imcalc.info"),
                 (rel / f"{name}_im_calc.log", f"{name}_im_calc.log")]
    else:
        raise ValueError(stage)
    return pairs if pairs[0][0].exists() else []


def check_source(stage, pairs):
    """Problems that make these outputs unfit to upload."""
    missing = [str(p) for p, _ in pairs if not p.is_file()]
    if missing:
        return [f"missing {missing}"]
    first = pairs[0][0]
    if stage == "HF":
        with open(first, "rb") as f:
            f.seek(224)
            model = f.read(64).split(b"\0")[0]
        with open(pairs[1][0], "rb") as f:
            f.seek(max(0, os.path.getsize(pairs[1][0]) - 65536))
            done = b"Simulation completed" in f.read()
        return ([] if model == HF_MODEL else [f"HF model {model.decode()}"]) + ([] if done else ["HF.log not completed"])
    if stage == "BB":
        n, nt = (int(v) for v in np.fromfile(first, "i4", 2))
        expected = 0x500 + n * 44 + n * nt * 12
        return [] if first.stat().st_size == expected else [f"BB.bin {first.stat().st_size} bytes, header implies {expected}"]
    return [] if first.stat().st_size > 0 else ["empty IM CSV"]


def sha256_range(f, offset, size):
    f.seek(offset)
    sha, left = hashlib.sha256(), size
    while left:
        data = f.read(min(CHUNK, left))
        if not data:
            raise IOError("tar ends inside a member")
        sha.update(data)
        left -= len(data)
    return sha.hexdigest()


def pack(stage, rel, pairs, out_dir, manifest_dir):
    final = out_dir / f"{rel.name}_{stage}.tar"
    record = manifest_dir / f"{final.name}.tsv"
    if final.exists():
        if record.exists():
            return "already packed"
        raise RuntimeError(f"{final} exists without a manifest record - refusing to touch it")
    partial = final.with_name(final.name + ".partial")
    rows = []
    with tarfile.open(partial, "w", format=tarfile.PAX_FORMAT) as tar:
        for source, member in pairs:
            info = tar.gettarinfo(str(source), arcname=member)
            info.mode = 0o644
            with open(source, "rb") as f:
                reader = HashingReader(f)
                tar.addfile(info, reader)
            rows.append((member, info.size, reader.sha.hexdigest(), str(source), int(source.stat().st_mtime)))
    # verify: every member hashes as its source did, and nothing else is in the tar
    with tarfile.open(partial, "r") as tar, open(partial, "rb") as raw:
        members = tar.getmembers()
        if [m.name for m in members] != [r[0] for r in rows]:
            raise RuntimeError(f"{partial}: members {[m.name for m in members]}")
        for m, (name, size, sha, _, _) in zip(members, rows):
            if m.size != size or sha256_range(raw, m.offset_data, m.size) != sha:
                raise RuntimeError(f"{partial}: member {name} does not match its source")
    tar_sha = sha256_range(open(partial, "rb"), 0, partial.stat().st_size)
    tar_size = partial.stat().st_size
    os.rename(partial, final)
    record.write_text("".join(f"{final.name}\t{tar_size}\t{tar_sha}\t{name}\t{size}\t{sha}\t{source}\t{mtime}\n"
                              for name, size, sha, source, mtime in rows))
    return f"packed {tar_size / 1e9:.1f} GB"


def main():
    stage, targets_file, staging = sys.argv[1], sys.argv[2], Path(sys.argv[3])
    targets = [Path(t) for t in Path(targets_file).read_text().split()]
    manifest_dir = staging / ".manifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    faults, failures = set(), 0
    for rel in targets:
        pairs = sources(stage, rel)
        if not pairs:
            print(f"{rel.name:22s} no {stage} outputs - skipped")
            continue
        problems = check_source(stage, pairs)
        if problems:
            print(f"{rel.name:22s} NOT PACKED: {'; '.join(problems)}")
            failures += 1
            continue
        out_dir = staging / stage / rel.parent.name
        out_dir.mkdir(parents=True, exist_ok=True)
        faults.add(rel.parent.name)
        try:
            result = pack(stage, rel, pairs, out_dir, manifest_dir)
        except Exception as e:  # noqa: BLE001 - reported, and the run carries on
            result, failures = f"FAILED: {e}", failures + 1
        print(f"{rel.name:22s} {result}", flush=True)
    for fault in sorted(faults):
        out_dir = staging / stage / fault
        rows = "".join(r.read_text() for r in sorted(manifest_dir.glob(f"{fault}*_{stage}.tar.tsv")))
        (out_dir / "MANIFEST.tsv").write_text(
            "tar\ttar_bytes\ttar_sha256\tmember\tmember_bytes\tmember_sha256\tsource_on_nesi\tsource_mtime\n" + rows)
        print(f"{out_dir}/MANIFEST.tsv: {rows.count(chr(10))} members")
    print("RESULT:", "OK" if not failures else f"{failures} failure(s)")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
