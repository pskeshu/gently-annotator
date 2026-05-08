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
    return {
        "dataset": dataset,
        "session": session,
        "embryo": embryo,
        "annotator": name,
        "stages": catalog.stages,
        "transitions": transitions,
        "notes": notes,
        "flag": flag,
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
