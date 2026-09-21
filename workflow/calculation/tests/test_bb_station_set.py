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


from workflow.calculation.bb_station_set import StationSet, resolve_station_set


def test_identical_sets_resolve_unchanged_in_hf_order():
    lf = np.array(["ccc", "aaa", "bbb"])
    hf = np.array(["aaa", "bbb", "ccc"])
    result = resolve_station_set(lf, hf)
    assert isinstance(result, StationSet)
    # HF order wins, not LF order and not sorted order.
    assert result.names.tolist() == ["aaa", "bbb", "ccc"]
    assert result.lf_idx.tolist() == [1, 2, 0]
    assert result.hf_idx.tolist() == [0, 1, 2]


def test_index_arrays_round_trip_to_the_right_names():
    lf = np.array(["ccc", "aaa", "bbb"])
    hf = np.array(["aaa", "bbb", "ccc"])
    result = resolve_station_set(lf, hf)
    assert lf[result.lf_idx].tolist() == result.names.tolist()
    assert hf[result.hf_idx].tolist() == result.names.tolist()


def test_duplicate_only_mismatch_resolves_to_full_hf_set():
    # The PalliserKai REL08 shape: LF has duplicates and a blank, but once
    # collapsed it covers exactly the HF set. Nothing is lost, so no flag.
    lf = np.array(["aaa", "bbb", "aaa", "", "ccc", "bbb"])
    hf = np.array(["aaa", "bbb", "ccc"])
    result = resolve_station_set(lf, hf)
    assert result.names.tolist() == ["aaa", "bbb", "ccc"]
    assert lf[result.lf_idx].tolist() == ["aaa", "bbb", "ccc"]
    assert any("de-duplicated" in line for line in result.report)


def test_genuine_subset_aborts_by_default():
    # The WellTeast shape: HF has a station LF genuinely lacks.
    lf = np.array(["aaa", "bbb", ""])
    hf = np.array(["aaa", "bbb", "320077e"])
    with pytest.raises(StationSetError, match="320077e"):
        resolve_station_set(lf, hf)


def test_genuine_subset_proceeds_with_allow_subset_and_names_the_loss():
    lf = np.array(["aaa", "bbb", ""])
    hf = np.array(["aaa", "bbb", "320077e"])
    result = resolve_station_set(lf, hf, allow_subset=True)
    assert result.names.tolist() == ["aaa", "bbb"]
    assert any("320077e" in line for line in result.report)


def test_station_only_in_lf_also_counts_as_a_mismatch():
    lf = np.array(["aaa", "bbb", "extra"])
    hf = np.array(["aaa", "bbb"])
    with pytest.raises(StationSetError, match="extra"):
        resolve_station_set(lf, hf)


def test_no_overlap_at_all_is_always_an_error():
    lf = np.array(["aaa"])
    hf = np.array(["bbb"])
    with pytest.raises(StationSetError):
        resolve_station_set(lf, hf, allow_subset=True)


def test_station_list_selects_exactly_those_names_in_file_order():
    lf = np.array(["aaa", "bbb", "ccc"])
    hf = np.array(["ccc", "bbb", "aaa"])
    result = resolve_station_set(lf, hf, station_list=["ccc", "aaa"])
    assert result.names.tolist() == ["ccc", "aaa"]
    assert lf[result.lf_idx].tolist() == ["ccc", "aaa"]
    assert hf[result.hf_idx].tolist() == ["ccc", "aaa"]


def test_station_list_drops_a_station_both_sides_have():
    # The WellTeast case for the six already-complete realisations: both
    # LF and HF hold 320077e, and only the explicit list removes it.
    lf = np.array(["aaa", "320077e", "bbb"])
    hf = np.array(["aaa", "320077e", "bbb"])
    result = resolve_station_set(lf, hf, station_list=["aaa", "bbb"])
    assert result.names.tolist() == ["aaa", "bbb"]
    assert "320077e" not in result.names.tolist()


def test_station_list_aborts_when_a_name_is_absent_from_lf():
    lf = np.array(["aaa", "bbb"])
    hf = np.array(["aaa", "bbb", "320077e"])
    with pytest.raises(StationSetError, match="320077e"):
        resolve_station_set(lf, hf, station_list=["aaa", "bbb", "320077e"])


def test_station_list_aborts_when_a_name_is_absent_from_hf():
    lf = np.array(["aaa", "bbb", "zzz"])
    hf = np.array(["aaa", "bbb"])
    with pytest.raises(StationSetError, match="zzz"):
        resolve_station_set(lf, hf, station_list=["aaa", "bbb", "zzz"])


def test_station_list_still_sees_through_lf_duplicates():
    lf = np.array(["aaa", "", "bbb", "aaa"])
    hf = np.array(["aaa", "bbb"])
    result = resolve_station_set(lf, hf, station_list=["bbb", "aaa"])
    assert result.names.tolist() == ["bbb", "aaa"]
    assert lf[result.lf_idx].tolist() == ["bbb", "aaa"]


def test_station_list_with_allow_subset_is_a_usage_error():
    lf = np.array(["aaa"])
    hf = np.array(["aaa"])
    with pytest.raises(StationSetError, match="mutually exclusive"):
        resolve_station_set(lf, hf, station_list=["aaa"], allow_subset=True)
