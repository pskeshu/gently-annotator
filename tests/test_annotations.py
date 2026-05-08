"""Storage layer tests — transitions, notes, flag CRUD + per-annotator separation."""

from __future__ import annotations

from pathlib import Path

import pytest

from annotator.annotations import AnnotationStore


@pytest.fixture
def store(tmp_path: Path) -> AnnotationStore:
    return AnnotationStore(tmp_path / "annotations.db")


# ----- transitions -----

def test_transition_upsert_and_list(store: AnnotationStore):
    store.upsert_transition("D", "S", "E", "bean", 10, "kesavan")
    rows = store.list_transitions("D", "S", "E", "kesavan")
    assert len(rows) == 1
    assert rows[0]["stage"] == "bean"
    assert rows[0]["timepoint"] == 10


def test_transition_upsert_replaces_per_stage(store: AnnotationStore):
    store.upsert_transition("D", "S", "E", "bean", 10, "kesavan")
    store.upsert_transition("D", "S", "E", "bean", 12, "kesavan")
    rows = store.list_transitions("D", "S", "E", "kesavan")
    assert len(rows) == 1
    assert rows[0]["timepoint"] == 12  # newest wins


def test_transition_per_annotator_isolation(store: AnnotationStore):
    store.upsert_transition("D", "S", "E", "bean", 10, "kesavan")
    store.upsert_transition("D", "S", "E", "bean", 14, "trisha")
    k = store.list_transitions("D", "S", "E", "kesavan")
    t = store.list_transitions("D", "S", "E", "trisha")
    assert k[0]["timepoint"] == 10
    assert t[0]["timepoint"] == 14


def test_transition_delete(store: AnnotationStore):
    store.upsert_transition("D", "S", "E", "bean", 10, "kesavan")
    store.delete_transition("D", "S", "E", "bean", "kesavan")
    assert store.list_transitions("D", "S", "E", "kesavan") == []


# ----- notes -----

def test_note_upsert_and_list(store: AnnotationStore):
    store.upsert_note("D", "S", "E", 42, "kesavan", "second cell division here")
    rows = store.list_notes("D", "S", "E", "kesavan")
    assert len(rows) == 1
    assert rows[0]["timepoint"] == 42
    assert rows[0]["note"] == "second cell division here"


def test_note_empty_string_deletes(store: AnnotationStore):
    store.upsert_note("D", "S", "E", 42, "kesavan", "scratch")
    store.upsert_note("D", "S", "E", 42, "kesavan", "   ")  # whitespace-only
    assert store.list_notes("D", "S", "E", "kesavan") == []


def test_note_per_annotator_isolation(store: AnnotationStore):
    store.upsert_note("D", "S", "E", 5, "kesavan", "looks like bean")
    store.upsert_note("D", "S", "E", 5, "trisha", "borderline")
    assert store.list_notes("D", "S", "E", "kesavan")[0]["note"] == "looks like bean"
    assert store.list_notes("D", "S", "E", "trisha")[0]["note"] == "borderline"


# ----- flag -----

def test_flag_upsert_and_get(store: AnnotationStore):
    store.upsert_flag("D", "S", "E", "kesavan", excluded=True, notes="out of focus")
    f = store.get_flag("D", "S", "E", "kesavan")
    assert f["excluded"] == 1
    assert f["notes"] == "out of focus"


def test_flag_upsert_replaces(store: AnnotationStore):
    store.upsert_flag("D", "S", "E", "kesavan", excluded=True)
    store.upsert_flag("D", "S", "E", "kesavan", excluded=False)
    assert store.get_flag("D", "S", "E", "kesavan")["excluded"] == 0


# ----- orientations -----

def test_orientation_axis_independent(store: AnnotationStore):
    store.upsert_orientation_axis("D", "S", "E", 10, "k", "ap", [0, 1, 0])
    rows = store.list_orientations("D", "S", "E", "k")
    assert len(rows) == 1
    assert rows[0]["ap_dir"] == [0, 1, 0]
    assert rows[0]["dv_dir"] is None
    # Add DV without affecting AP.
    store.upsert_orientation_axis("D", "S", "E", 10, "k", "dv", [1, 0, 0])
    rows = store.list_orientations("D", "S", "E", "k")
    assert rows[0]["ap_dir"] == [0, 1, 0]
    assert rows[0]["dv_dir"] == [1, 0, 0]


def test_orientation_clear_one_axis_keeps_other(store: AnnotationStore):
    store.upsert_orientation_axis("D", "S", "E", 10, "k", "ap", [0, 1, 0])
    store.upsert_orientation_axis("D", "S", "E", 10, "k", "dv", [1, 0, 0])
    store.upsert_orientation_axis("D", "S", "E", 10, "k", "ap", None)
    rows = store.list_orientations("D", "S", "E", "k")
    assert len(rows) == 1
    assert rows[0]["ap_dir"] is None
    assert rows[0]["dv_dir"] == [1, 0, 0]


def test_orientation_row_garbage_collected(store: AnnotationStore):
    """When both axes are NULL and notes are empty, the row is removed."""
    store.upsert_orientation_axis("D", "S", "E", 10, "k", "ap", [0, 1, 0])
    store.upsert_orientation_axis("D", "S", "E", 10, "k", "ap", None)
    assert store.list_orientations("D", "S", "E", "k") == []


def test_orientation_per_annotator(store: AnnotationStore):
    store.upsert_orientation_axis("D", "S", "E", 5, "kesavan", "ap", [1, 0, 0])
    store.upsert_orientation_axis("D", "S", "E", 5, "trisha", "ap", [0, 1, 0])
    k = store.list_orientations("D", "S", "E", "kesavan")
    t = store.list_orientations("D", "S", "E", "trisha")
    assert k[0]["ap_dir"] == [1, 0, 0]
    assert t[0]["ap_dir"] == [0, 1, 0]


def test_unreliable_range_add_list_delete(store: AnnotationStore):
    rid = store.add_unreliable_range("D", "S", "E", 50, 80, "k", notes="twitching")
    rows = store.list_unreliable_ranges("D", "S", "E", "k")
    assert len(rows) == 1
    assert (rows[0]["start_tp"], rows[0]["end_tp"]) == (50, 80)
    assert rows[0]["notes"] == "twitching"
    store.delete_unreliable_range(rid, "k")
    assert store.list_unreliable_ranges("D", "S", "E", "k") == []


def test_unreliable_range_swaps_start_end(store: AnnotationStore):
    """Caller may pass them in any order; storage normalises."""
    store.add_unreliable_range("D", "S", "E", 80, 50, "k")
    rows = store.list_unreliable_ranges("D", "S", "E", "k")
    assert (rows[0]["start_tp"], rows[0]["end_tp"]) == (50, 80)
