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
