"""Volume route: returns base64 uint8 + shape + voxel_size_um for the 3D viewer."""

from __future__ import annotations

import base64
import logging

from fastapi import APIRouter, HTTPException, Request

from ..volume_io import load_volume, normalize_for_3d, preprocess

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")


@router.get(
    "/datasets/{dataset}/sessions/{session}/embryos/{embryo}/volumes/{timepoint}"
)
async def get_volume_raw(
    dataset: str, session: str, embryo: str, timepoint: int, request: Request
):
    catalog = request.app.state.catalog
    cache = request.app.state.volume_cache
    cache_max = request.app.state.volume_cache_max
    cfg = request.app.state.config

    path = catalog.get_timepoint_path(dataset, session, embryo, timepoint)
    if path is None:
        raise HTTPException(
            status_code=404,
            detail=f"No timepoint {timepoint} for {dataset}/{session}/{embryo}",
        )

    cache_key = (dataset, session, embryo, timepoint)
    if cache_key in cache:
        cache.move_to_end(cache_key)
        return cache[cache_key]

    try:
        vol = load_volume(path)
        vol = preprocess(vol)
        vol_uint8 = normalize_for_3d(vol)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        logger.exception("Failed to load volume %s", path)
        raise HTTPException(status_code=500, detail=f"Failed to load volume: {exc}")

    voxel_size = cfg.get("voxel_size_um", [1.0, 0.1625, 0.1625])
    response = {
        "dataset": dataset,
        "session": session,
        "embryo": embryo,
        "timepoint": timepoint,
        "shape": list(vol_uint8.shape),
        "voxel_size_um": voxel_size,
        "data": base64.b64encode(vol_uint8.tobytes()).decode("ascii"),
    }

    cache[cache_key] = response
    while len(cache) > cache_max:
        cache.popitem(last=False)
    return response
