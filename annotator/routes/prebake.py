"""Pre-bake routes — start a background bake of all timepoints for an
embryo, and poll its progress.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/prebake")


class StartBody(BaseModel):
    dataset: str
    session: str
    embryo: str


@router.post("/start")
async def start(body: StartBody, request: Request):
    catalog = request.app.state.catalog
    sm = catalog.get_session(body.dataset, body.session)
    if sm is None or body.embryo not in sm.embryos:
        raise HTTPException(status_code=404, detail="No such embryo")
    em = sm.embryos[body.embryo]
    tp_to_path = {tp.timepoint: tp.path for tp in em.timepoints}
    if not tp_to_path:
        raise HTTPException(status_code=400, detail="Embryo has no timepoints")
    status = request.app.state.prebake.start(
        body.dataset, body.session, body.embryo, tp_to_path
    )
    return status


@router.get("/status")
async def status(
    request: Request,
    dataset: Optional[str] = None,
    session: Optional[str] = None,
    embryo: Optional[str] = None,
):
    if not dataset or not session or not embryo:
        raise HTTPException(
            status_code=400, detail="dataset, session, embryo are required"
        )
    return request.app.state.prebake.status(dataset, session, embryo)


@router.post("/cancel")
async def cancel(request: Request):
    request.app.state.prebake.cancel()
    return {"ok": True}
