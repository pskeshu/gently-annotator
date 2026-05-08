"""On-disk cache of preprocessed uint8 voxel arrays.

Each cache file is one timepoint's preprocessed volume — exactly the bytes
that get streamed to the browser. Avoids redoing the
TIF-decode + percentile + Z-blur + uint8-quantize pipeline on every fetch
(saves ~1.5 s per request once warm).

File layout (little-endian):

    offset 0   : magic   = b'GAV1'    (Gently Annotator Volume v1)
    offset 4   : version = uint32     (1)
    offset 8   : zd, h, w = 3 x uint32  (volume shape)
    offset 20  : dz, dy, dx = 3 x float32  (voxel size in microns)
    offset 32  : reserved = uint32    (0)
    offset 36  : voxels   = zd * h * w bytes uint8

Writes are atomic: stage to ``<path>.tmp``, then rename. So a worker
killed mid-write never leaves a torn cache file.
"""

from __future__ import annotations

import logging
import os
import struct
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

MAGIC = b"GAV1"
VERSION = 1
HEADER_FMT = "<4sIIII3fI"
HEADER_SIZE = struct.calcsize(HEADER_FMT)
assert HEADER_SIZE == 36


def cache_path_for(
    cache_root: Path | str,
    dataset: str,
    session: str,
    embryo: str,
    timepoint: int,
) -> Path:
    """Stable path for a single timepoint's preview sidecar."""
    return Path(cache_root) / dataset / session / embryo / f"t{int(timepoint):04d}.u8"


def write_sidecar(
    path: Path | str,
    vol_uint8: np.ndarray,
    voxel_size_um: tuple[float, float, float] | list[float],
) -> None:
    """Write a sidecar atomically. Overwrites if the target exists."""
    if vol_uint8.dtype != np.uint8:
        raise ValueError(f"Expected uint8 voxels, got {vol_uint8.dtype}")
    if vol_uint8.ndim != 3:
        raise ValueError(f"Expected 3D volume, got shape {vol_uint8.shape}")

    zd, h, w = vol_uint8.shape
    dz, dy, dx = (float(x) for x in voxel_size_um)
    header = struct.pack(HEADER_FMT, MAGIC, VERSION, zd, h, w, dz, dy, dx, 0)

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as f:
        f.write(header)
        f.write(vol_uint8.tobytes())
    os.replace(tmp, path)


def read_sidecar(path: Path | str) -> tuple[bytes, list[int], list[float]]:
    """Return (raw_uint8_bytes, [zd, h, w], [dz, dy, dx])."""
    path = Path(path)
    with open(path, "rb") as f:
        header = f.read(HEADER_SIZE)
        if len(header) != HEADER_SIZE:
            raise ValueError(f"Truncated header in {path}")
        magic, version, zd, h, w, dz, dy, dx, _reserved = struct.unpack(
            HEADER_FMT, header
        )
        if magic != MAGIC:
            raise ValueError(f"Bad magic {magic!r} in {path}")
        if version != VERSION:
            raise ValueError(f"Unsupported version {version} in {path}")
        body = f.read()
        expected = int(zd) * int(h) * int(w)
        if len(body) != expected:
            raise ValueError(
                f"Truncated body in {path}: got {len(body)}, expected {expected}"
            )
    return body, [int(zd), int(h), int(w)], [float(dz), float(dy), float(dx)]


def is_complete(path: Path | str) -> bool:
    """Cheap existence + size sanity check (header + at least one voxel)."""
    p = Path(path)
    try:
        return p.is_file() and p.stat().st_size > HEADER_SIZE
    except OSError:
        return False
