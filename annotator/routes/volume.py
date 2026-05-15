"""Volume route — returns the preprocessed uint8 voxel array as a binary
octet-stream, with shape and voxel_size in HTTP headers.

We used to return base64 inside JSON, which doubled the payload (33%
inflation) and forced the browser to do JSON.parse + atob on a 35 MB
string for every timepoint switch. Binary saves both the inflation and
the parse step — visible improvement in scrubbing latency.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from ..preview_cache import (
    cache_path_for,
    is_complete as sidecar_is_complete,
    read_sidecar,
    write_sidecar,
)
from ..volume_io import (
    DEFAULT_BG_OFFSET,
    DEFAULT_VIEW,
    load_volume,
    normalize_for_3d,
    preprocess,
    z_blur_sigma_for_session,
)


def _dataset_preprocess(cfg: dict, dataset: str) -> dict:
    """Per-dataset preprocess options (view crop, bg offset).

    Looked up in cfg["datasets"][dataset]["preprocess"]; falls back to
    the global volume_io defaults (left view, bg=100).
    """
    ds_cfg = cfg.get("datasets", {}).get(dataset, {}) or {}
    pp = ds_cfg.get("preprocess", {}) or {}
    return {
        "view": pp.get("view", DEFAULT_VIEW),
        "bg_offset": int(pp.get("bg_offset", DEFAULT_BG_OFFSET)),
    }

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
        cached = cache[cache_key]
    else:
        cache_root = Path(cfg.get("cache_root", "preview_cache"))
        sidecar = cache_path_for(cache_root, dataset, session, embryo, timepoint)
        cached = None

        # Try the disk cache first — orders of magnitude faster than
        # re-running the full preprocess pipeline.
        if sidecar_is_complete(sidecar):
            try:
                body, shape, voxel_size = read_sidecar(sidecar)
                cached = {
                    "shape": shape,
                    "voxel_size_um": voxel_size,
                    "bytes": body,
                }
            except Exception as exc:
                logger.warning("Bad sidecar %s, will regenerate: %s", sidecar, exc)
                cached = None

        if cached is None:
            try:
                vol = load_volume(path)
                pp = _dataset_preprocess(cfg, dataset)
                vol = preprocess(vol, view=pp["view"], bg_offset=pp["bg_offset"])
                vol_uint8 = normalize_for_3d(
                    vol, z_blur_sigma=z_blur_sigma_for_session(session),
                )
            except FileNotFoundError as exc:
                raise HTTPException(status_code=404, detail=str(exc))
            except Exception as exc:
                logger.exception("Failed to load volume %s", path)
                raise HTTPException(status_code=500, detail=f"Failed to load volume: {exc}")

            voxel_size = cfg.get("voxel_size_um", [1.0, 0.1625, 0.1625])
            cached = {
                "shape": list(vol_uint8.shape),
                "voxel_size_um": voxel_size,
                "bytes": vol_uint8.tobytes(),
            }
            try:
                write_sidecar(sidecar, vol_uint8, voxel_size)
            except OSError as exc:
                logger.warning("Could not write sidecar %s: %s", sidecar, exc)

        cache[cache_key] = cached
        while len(cache) > cache_max:
            cache.popitem(last=False)

    headers = {
        "X-Volume-Shape": ",".join(str(x) for x in cached["shape"]),
        "X-Volume-Voxel-Size-Um": ",".join(str(x) for x in cached["voxel_size_um"]),
        "X-Volume-Dataset": dataset,
        "X-Volume-Session": session,
        "X-Volume-Embryo": embryo,
        "X-Volume-Timepoint": str(timepoint),
        "Cache-Control": "no-store",
    }
    return Response(
        content=cached["bytes"],
        media_type="application/octet-stream",
        headers=headers,
    )
