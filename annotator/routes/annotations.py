"""Annotation routes — transitions, notes, exclude flag.

Path shape:
  /api/annotations/{dataset}/{session}/{embryo}              GET (bundled)
  /api/annotations/{dataset}/{session}/{embryo}/transitions  POST
  /api/annotations/{dataset}/{session}/{embryo}/transitions/{stage}  DELETE
  /api/annotations/{dataset}/{session}/{embryo}/notes/{timepoint}    POST/DELETE
  /api/annotations/{dataset}/{session}/{embryo}/flag         POST

The `annotator` name is a query param on GET and a body field on writes.
There's no auth — this is a single-user-per-cookie setup; the annotator
field is for separating each person's labels, not access control.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/annotations")


@router.get("/annotators")
async def list_known_annotators(request: Request):
    """Distinct annotator names known to the DB. Drives the view-as picker."""
    store = request.app.state.store
    return {"annotators": store.list_known_annotators()}


@router.get("/summary")
async def get_summary(request: Request, annotator: Optional[str] = None):
    """One row per (dataset, session, embryo) where this annotator has any data.

    Used by the sidebar to show which sessions/embryos already have work.
    Cheap — three small GROUP BYs.
    """
    name = _require_annotator(annotator)
    store = request.app.state.store
    items = store.summary_for_annotator(name)
    return {"annotator": name, "items": items}


def _check_path(catalog, dataset: str, session: str, embryo: str) -> None:
    sm = catalog.get_session(dataset, session)
    if sm is None or embryo not in sm.embryos:
        raise HTTPException(status_code=404, detail="No such embryo")


def _require_annotator(name: Optional[str]) -> str:
    if not name or not name.strip():
        raise HTTPException(status_code=400, detail="annotator name required")
    return name.strip()


# ---- request bodies ----

class TransitionBody(BaseModel):
    annotator: str
    stage: str
    timepoint: int
    notes: Optional[str] = None


class NoteBody(BaseModel):
    annotator: str
    note: str = Field(..., description="free-text morphology note")


class FlagBody(BaseModel):
    annotator: str
    excluded: bool
    notes: Optional[str] = None


class OrientationBody(BaseModel):
    annotator: str
    axis: str  # 'ap' or 'dv'
    direction: Optional[list[float]] = None  # null clears it; else unit vector


class UnreliableRangeBody(BaseModel):
    annotator: str
    start_tp: int
    end_tp: int
    notes: Optional[str] = None


class ViewNoteAddBody(BaseModel):
    annotator: str
    timepoint: int
    view_params: dict
    note: str
    tag: Optional[str] = None


class ViewNotePatchBody(BaseModel):
    annotator: str
    note: Optional[str] = None
    tag: Optional[str] = None
    view_params: Optional[dict] = None


# ---- routes ----

@router.get("/{dataset}/{session}/{embryo}")
async def get_bundle(
    dataset: str,
    session: str,
    embryo: str,
    request: Request,
    annotator: Optional[str] = None,
):
    """Return all of one annotator's labels for an embryo in a single payload."""
    catalog = request.app.state.catalog
    store = request.app.state.store
    _check_path(catalog, dataset, session, embryo)
    name = _require_annotator(annotator)

    transitions = store.list_transitions(dataset, session, embryo, name)
    notes = store.list_notes(dataset, session, embryo, name)
    flag = store.get_flag(dataset, session, embryo, name)
    orientations = store.list_orientations(dataset, session, embryo, name)
    unreliable = store.list_unreliable_ranges(dataset, session, embryo, name)
    view_notes = store.list_view_notes(dataset, session, embryo, name)
    return {
        "dataset": dataset,
        "session": session,
        "embryo": embryo,
        "annotator": name,
        "stages": catalog.stages,
        "transitions": transitions,
        "notes": notes,
        "flag": flag,
        "orientations": orientations,
        "unreliable_ranges": unreliable,
        "view_notes": view_notes,
    }


@router.post("/{dataset}/{session}/{embryo}/transitions")
async def upsert_transition(
    dataset: str, session: str, embryo: str, body: TransitionBody, request: Request
):
    catalog = request.app.state.catalog
    store = request.app.state.store
    _check_path(catalog, dataset, session, embryo)
    name = _require_annotator(body.annotator)
    if body.stage not in catalog.stages:
        raise HTTPException(status_code=400, detail=f"Unknown stage {body.stage!r}")
    store.upsert_transition(
        dataset, session, embryo, body.stage, body.timepoint, name, body.notes
    )
    return {"ok": True}


@router.delete("/{dataset}/{session}/{embryo}/transitions/{stage}")
async def delete_transition(
    dataset: str,
    session: str,
    embryo: str,
    stage: str,
    request: Request,
    annotator: Optional[str] = None,
):
    catalog = request.app.state.catalog
    store = request.app.state.store
    _check_path(catalog, dataset, session, embryo)
    name = _require_annotator(annotator)
    store.delete_transition(dataset, session, embryo, stage, name)
    return {"ok": True}


@router.post("/{dataset}/{session}/{embryo}/notes/{timepoint}")
async def upsert_note(
    dataset: str,
    session: str,
    embryo: str,
    timepoint: int,
    body: NoteBody,
    request: Request,
):
    catalog = request.app.state.catalog
    store = request.app.state.store
    _check_path(catalog, dataset, session, embryo)
    name = _require_annotator(body.annotator)
    store.upsert_note(dataset, session, embryo, timepoint, name, body.note)
    return {"ok": True}


@router.delete("/{dataset}/{session}/{embryo}/notes/{timepoint}")
async def delete_note(
    dataset: str,
    session: str,
    embryo: str,
    timepoint: int,
    request: Request,
    annotator: Optional[str] = None,
):
    catalog = request.app.state.catalog
    store = request.app.state.store
    _check_path(catalog, dataset, session, embryo)
    name = _require_annotator(annotator)
    store.delete_note(dataset, session, embryo, timepoint, name)
    return {"ok": True}


@router.post("/{dataset}/{session}/{embryo}/flag")
async def upsert_flag(
    dataset: str, session: str, embryo: str, body: FlagBody, request: Request
):
    catalog = request.app.state.catalog
    store = request.app.state.store
    _check_path(catalog, dataset, session, embryo)
    name = _require_annotator(body.annotator)
    store.upsert_flag(dataset, session, embryo, name, body.excluded, body.notes)
    return {"ok": True}


# ---- orientation ----

@router.post("/{dataset}/{session}/{embryo}/orientation/{timepoint}")
async def upsert_orientation(
    dataset: str,
    session: str,
    embryo: str,
    timepoint: int,
    body: OrientationBody,
    request: Request,
):
    catalog = request.app.state.catalog
    store = request.app.state.store
    _check_path(catalog, dataset, session, embryo)
    name = _require_annotator(body.annotator)
    if body.axis not in ("ap", "dv"):
        raise HTTPException(status_code=400, detail=f"Unknown axis {body.axis!r}")
    store.upsert_orientation_axis(
        dataset, session, embryo, timepoint, name, body.axis, body.direction
    )
    return {"ok": True}


@router.delete("/{dataset}/{session}/{embryo}/orientation/{timepoint}")
async def clear_orientation(
    dataset: str,
    session: str,
    embryo: str,
    timepoint: int,
    request: Request,
    annotator: Optional[str] = None,
):
    catalog = request.app.state.catalog
    store = request.app.state.store
    _check_path(catalog, dataset, session, embryo)
    name = _require_annotator(annotator)
    store.clear_orientation(dataset, session, embryo, timepoint, name)
    return {"ok": True}


@router.post("/{dataset}/{session}/{embryo}/unreliable")
async def add_unreliable(
    dataset: str,
    session: str,
    embryo: str,
    body: UnreliableRangeBody,
    request: Request,
):
    catalog = request.app.state.catalog
    store = request.app.state.store
    _check_path(catalog, dataset, session, embryo)
    name = _require_annotator(body.annotator)
    rid = store.add_unreliable_range(
        dataset, session, embryo, body.start_tp, body.end_tp, name, body.notes
    )
    return {"ok": True, "id": rid}


@router.delete("/{dataset}/{session}/{embryo}/unreliable/{range_id}")
async def delete_unreliable(
    dataset: str,
    session: str,
    embryo: str,
    range_id: int,
    request: Request,
    annotator: Optional[str] = None,
):
    catalog = request.app.state.catalog
    store = request.app.state.store
    _check_path(catalog, dataset, session, embryo)
    name = _require_annotator(annotator)
    store.delete_unreliable_range(range_id, name)
    return {"ok": True}


# ---- view notes ----

@router.post("/{dataset}/{session}/{embryo}/view-notes")
async def add_view_note(
    dataset: str,
    session: str,
    embryo: str,
    body: ViewNoteAddBody,
    request: Request,
):
    catalog = request.app.state.catalog
    store = request.app.state.store
    _check_path(catalog, dataset, session, embryo)
    name = _require_annotator(body.annotator)
    rid = store.add_view_note(
        dataset, session, embryo, body.timepoint, name,
        view_params=body.view_params, note=body.note, tag=body.tag,
    )
    return {"ok": True, "id": rid}


@router.patch("/{dataset}/{session}/{embryo}/view-notes/{note_id}")
async def patch_view_note(
    dataset: str,
    session: str,
    embryo: str,
    note_id: int,
    body: ViewNotePatchBody,
    request: Request,
):
    catalog = request.app.state.catalog
    store = request.app.state.store
    _check_path(catalog, dataset, session, embryo)
    name = _require_annotator(body.annotator)
    store.update_view_note(
        note_id, name,
        note=body.note, tag=body.tag, view_params=body.view_params,
    )
    return {"ok": True}


@router.delete("/{dataset}/{session}/{embryo}/view-notes/{note_id}")
async def delete_view_note(
    dataset: str,
    session: str,
    embryo: str,
    note_id: int,
    request: Request,
    annotator: Optional[str] = None,
):
    catalog = request.app.state.catalog
    store = request.app.state.store
    _check_path(catalog, dataset, session, embryo)
    name = _require_annotator(annotator)
    store.delete_view_note(note_id, name)
    return {"ok": True}
