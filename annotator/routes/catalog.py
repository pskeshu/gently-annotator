"""Catalog browse routes: datasets, sessions, embryos, timepoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/api")


@router.get("/datasets")
async def list_datasets(request: Request):
    catalog = request.app.state.catalog
    return {
        "stages": catalog.stages,
        "datasets": [
            {
                "name": d.name,
                "schema": d.schema,
                "root": str(d.root) if d.root else None,
                "available": d.root is not None,
            }
            for d in catalog.list_datasets()
        ],
    }


@router.get("/datasets/{dataset}/sessions")
async def list_sessions(dataset: str, request: Request, include_empty: bool = False):
    """Sessions with embryo/timepoint counts, sorted by data volume.

    By default skips sessions with no embryo data (failed runs, empty test
    sessions). Pass include_empty=true to see them all.
    """
    catalog = request.app.state.catalog
    if dataset not in catalog.datasets:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset!r} not found")
    summaries = catalog.list_session_summaries(dataset)
    total = len(summaries)
    if not include_empty:
        summaries = [s for s in summaries if s.embryo_count > 0]
    return {
        "dataset": dataset,
        "total_sessions": total,
        "shown": len(summaries),
        "hidden_empty": total - len(summaries) if not include_empty else 0,
        "sessions": [
            {
                "session_id": s.session_id,
                "embryo_count": s.embryo_count,
                "timepoint_count": s.timepoint_count,
                "embryo_ids": list(s.embryo_ids),
            }
            for s in summaries
        ],
    }


@router.get("/datasets/{dataset}/sessions/{session}/embryos")
async def list_embryos(dataset: str, session: str, request: Request):
    catalog = request.app.state.catalog
    sm = catalog.get_session(dataset, session)
    if sm is None:
        raise HTTPException(status_code=404, detail=f"Session {session!r} not found")
    return {
        "dataset": dataset,
        "session": session,
        "embryos": [
            {
                "embryo_id": eid,
                "timepoint_count": len(em.timepoints),
                "first_timepoint": em.timepoints[0].timepoint if em.timepoints else None,
                "last_timepoint": em.timepoints[-1].timepoint if em.timepoints else None,
            }
            for eid, em in sorted(sm.embryos.items())
        ],
    }


@router.get("/datasets/{dataset}/sessions/{session}/embryos/{embryo}/timepoints")
async def list_timepoints(dataset: str, session: str, embryo: str, request: Request):
    catalog = request.app.state.catalog
    sm = catalog.get_session(dataset, session)
    if sm is None or embryo not in sm.embryos:
        raise HTTPException(status_code=404, detail="Embryo not found")
    em = sm.embryos[embryo]
    return {
        "dataset": dataset,
        "session": session,
        "embryo": embryo,
        "timepoints": [t.timepoint for t in em.timepoints],
    }
