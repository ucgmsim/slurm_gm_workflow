import struct

import numpy as np
import pytest

from workflow.calculation.verification.compare_bb_stations import (
    HEAD_SIZE,
    STATION_DTYPE,
    compare,
    read_bb_header,
)


def write_bb(path, names, nt=100):
    """Write just enough of a real BB.bin for the header reader."""
    stations = np.zeros(len(names), dtype=STATION_DTYPE)
    stations["name"] = [n.encode() for n in names]
    stations["vsite"] = 1.0
    with open(path, "wb") as f:
        f.write(struct.pack("<ii", len(names), nt))
        f.write(b"\0" * (HEAD_SIZE - 8))
        stations.tofile(f)
    return path


def test_reads_nstat_nt_and_names(tmp_path):
    p = write_bb(tmp_path / "BB.bin", ["aaa", "bbb", "ccc"], nt=42)
    header = read_bb_header(p)
    assert header.nstat == 3
    assert header.nt == 42
    assert header.names == ["aaa", "bbb", "ccc"]


def test_identical_files_compare_equal(tmp_path):
    a = write_bb(tmp_path / "a.bin", ["aaa", "bbb"])
    b = write_bb(tmp_path / "b.bin", ["aaa", "bbb"])
    ok, differences = compare(a, b)
    assert ok
    assert differences == []


def test_different_station_count_is_reported(tmp_path):
    a = write_bb(tmp_path / "a.bin", ["aaa", "bbb"])
    b = write_bb(tmp_path / "b.bin", ["aaa"])
    ok, differences = compare(a, b)
    assert not ok
    assert any("nstat" in d for d in differences)


def test_different_station_order_is_reported(tmp_path):
    a = write_bb(tmp_path / "a.bin", ["aaa", "bbb"])
    b = write_bb(tmp_path / "b.bin", ["bbb", "aaa"])
    ok, differences = compare(a, b)
    assert not ok
    assert any("order" in d for d in differences)


def test_different_nt_is_reported(tmp_path):
    a = write_bb(tmp_path / "a.bin", ["aaa"], nt=100)
    b = write_bb(tmp_path / "b.bin", ["aaa"], nt=200)
    ok, differences = compare(a, b)
    assert not ok
    assert any("nt" in d for d in differences)
