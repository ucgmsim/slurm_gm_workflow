import numpy as np
import pytest

from workflow.calculation.bb_station_set import (
    StationSetError,
    first_occurrence_indices,
)


def test_clean_names_are_all_kept_in_order():
    names = np.array(["aaa", "bbb", "ccc"])
    keep, n_blank, n_dup = first_occurrence_indices(names)
    assert keep.tolist() == [0, 1, 2]
    assert (n_blank, n_dup) == (0, 0)


def test_duplicate_keeps_first_occurrence():
    names = np.array(["aaa", "bbb", "aaa", "ccc"])
    keep, n_blank, n_dup = first_occurrence_indices(names)
    assert keep.tolist() == [0, 1, 3]
    assert (n_blank, n_dup) == (0, 1)


def test_blank_names_are_dropped():
    names = np.array(["aaa", "", "bbb", ""])
    keep, n_blank, n_dup = first_occurrence_indices(names)
    assert keep.tolist() == [0, 2]
    assert (n_blank, n_dup) == (2, 0)


def test_blanks_and_duplicates_together():
    # The PalliserKai REL08 shape in miniature.
    names = np.array(["aaa", "bbb", "aaa", "", "bbb", "ccc"])
    keep, n_blank, n_dup = first_occurrence_indices(names)
    assert keep.tolist() == [0, 1, 5]
    assert (n_blank, n_dup) == (1, 2)


def test_returns_int64_so_it_can_index_large_arrays():
    keep, _, _ = first_occurrence_indices(np.array(["aaa"]))
    assert keep.dtype == np.int64


def test_empty_input_is_empty_output():
    keep, n_blank, n_dup = first_occurrence_indices(np.array([], dtype="<U8"))
    assert keep.tolist() == []
    assert (n_blank, n_dup) == (0, 0)


from workflow.calculation.bb_station_set import read_station_list


def test_reads_one_name_per_line(tmp_path):
    p = tmp_path / "stations.txt"
    p.write_text("aaa\nbbb\nccc\n")
    assert read_station_list(p) == ["aaa", "bbb", "ccc"]


def test_ignores_comments_and_blank_lines(tmp_path):
    p = tmp_path / "stations.txt"
    p.write_text("# canonical WellTeast set\n\naaa\n  bbb  \n\nccc  # trailing\n")
    assert read_station_list(p) == ["aaa", "bbb", "ccc"]


def test_preserves_file_order_not_sorted(tmp_path):
    p = tmp_path / "stations.txt"
    p.write_text("ccc\naaa\nbbb\n")
    assert read_station_list(p) == ["ccc", "aaa", "bbb"]


def test_rejects_empty_file(tmp_path):
    p = tmp_path / "stations.txt"
    p.write_text("# nothing but a comment\n")
    with pytest.raises(StationSetError, match="no station names"):
        read_station_list(p)


def test_rejects_repeated_name(tmp_path):
    p = tmp_path / "stations.txt"
    p.write_text("aaa\nbbb\naaa\n")
    with pytest.raises(StationSetError, match="aaa"):
        read_station_list(p)
