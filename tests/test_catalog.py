"""Smoke test: catalog can scan a real dataset on nearline.

Runs only if the configured nearline path is reachable. Otherwise skipped.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from annotator.catalog import Catalog


CONFIG = Path(__file__).resolve().parent.parent / "config.yaml"


@pytest.fixture(scope="module")
def catalog() -> Catalog:
    cfg = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    return Catalog(cfg["datasets"], cfg["stages"])


def test_datasets_loaded(catalog: Catalog):
    assert "Gently" in catalog.datasets
    assert "Gently2" in catalog.datasets


def test_gently_session_listing(catalog: Catalog):
    if catalog.datasets["Gently"].root is None:
        pytest.skip("Gently root not reachable")
    sessions = catalog.list_sessions("Gently")
    assert len(sessions) > 0


def test_gently_session_with_embryos(catalog: Catalog):
    if catalog.datasets["Gently"].root is None:
        pytest.skip("Gently root not reachable")
    sessions = catalog.list_sessions("Gently")
    # find a session that actually has embryos
    found = None
    for s in sessions:
        sm = catalog.get_session("Gently", s)
        if sm and sm.embryos:
            found = sm
            break
    assert found is not None, "expected at least one Gently session with embryos"
    # Each embryo should have at least one timepoint
    for em in found.embryos.values():
        assert len(em.timepoints) > 0


def test_session_summaries_sorted_by_data(catalog: Catalog):
    if catalog.datasets["Gently"].root is None:
        pytest.skip("Gently root not reachable")
    summaries = catalog.list_session_summaries("Gently")
    assert len(summaries) > 0
    # Sessions with embryos must come before empties.
    seen_empty = False
    for s in summaries:
        if s.embryo_count == 0:
            seen_empty = True
        elif seen_empty:
            pytest.fail(f"non-empty session {s.session_id} appears after an empty one")
    # Within the non-empty prefix, embryo_count is non-increasing.
    non_empty = [s for s in summaries if s.embryo_count > 0]
    for a, b in zip(non_empty, non_empty[1:]):
        assert (a.embryo_count, a.timepoint_count) >= (b.embryo_count, b.timepoint_count)


def test_session_summary_cached(catalog: Catalog):
    if catalog.datasets["Gently"].root is None:
        pytest.skip("Gently root not reachable")
    s1 = catalog.list_session_summaries("Gently")
    s2 = catalog.list_session_summaries("Gently")
    assert s1 is s2  # same object — second call hit the cache
