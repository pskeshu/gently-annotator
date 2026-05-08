"""Read a TIF volume and normalize it for the raymarched 3D viewer.

Pipeline per request:
    raw TIF
      └─► load_volume()    drop channel dim if present
      └─► preprocess()     crop to one view (X-half), subtract bg
      └─► normalize_for_3d() percentile-stretch, Z-blur, uint8 quantize

The view crop and bg subtraction match the diSPIM acquisition convention
in this dataset: each volume holds two views concatenated along X (full
width 2048 = 1024 view A + 1024 view B). The left half (X[:1024]) is
the strong-signal view; the right is the dim orthogonal one. Both views
sit on a constant ~100 ADU camera offset which we subtract before
normalization so percentile-stretching uses the actual signal range.

Normalization details (lifted from gently origin/main commit 6d3e06a):

- Percentile (1, 99) contrast stretch, not min/max — robust to outlier
  bright pixels that would otherwise wash out the volume.
- Gaussian blur along Z (sigma=1.0) — kills venetian-blind banding at
  side angles where the slice spacing is otherwise visible.
- Quantize to uint8 for transport; the shader does the rest.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import tifffile
from scipy import ndimage

logger = logging.getLogger(__name__)

# Preprocessing defaults. The two views are concatenated along X with the
# brighter (objective-aligned) view on the left. Background offset is the
# camera's dark-current pedestal — empirical mean is ~100–105 ADU.
DEFAULT_VIEW = "left"          # 'left' or 'right'
DEFAULT_BG_OFFSET = 100        # ADU subtracted before normalization


def load_volume(path: Path) -> np.ndarray:
    """Read a TIF and return a 3D (Z, Y, X) array.

    diSPIM volumes are stored 4D as (C, Z, Y, X) with C=1 on the older
    capture path, or already 3D on the newer one. Anything else is an error.
    """
    vol = tifffile.imread(str(path))
    if vol.ndim == 4:
        if vol.shape[0] != 1:
            logger.warning("Volume %s has %d channels; using channel 0", path, vol.shape[0])
        vol = vol[0]
    elif vol.ndim != 3:
        raise ValueError(f"Volume {path} has shape {vol.shape}; expected 3D or 4D")
    return vol


def preprocess(
    vol: np.ndarray,
    view: str = DEFAULT_VIEW,
    bg_offset: int = DEFAULT_BG_OFFSET,
) -> np.ndarray:
    """Crop to one diSPIM view along X, then subtract camera offset.

    Halves the X axis (so a 50×512×2048 input becomes 50×512×1024) and
    subtracts `bg_offset` with negative-clip. Result is non-negative
    in the same dtype family as the input (cast up to int32 if needed
    so uint16 inputs don't underflow).
    """
    if vol.ndim != 3:
        raise ValueError(f"Expected 3D volume, got shape {vol.shape}")

    x = vol.shape[2]
    if x % 2 != 0:
        raise ValueError(f"X dimension {x} is odd; cannot split into two views")
    half = x // 2

    if view == "left":
        view_vol = vol[:, :, :half]
    elif view == "right":
        view_vol = vol[:, :, half:]
    else:
        raise ValueError(f"view must be 'left' or 'right', got {view!r}")

    # Promote to int32 so uint16 inputs don't wrap when subtracting.
    out = view_vol.astype(np.int32, copy=False) - int(bg_offset)
    np.clip(out, 0, None, out=out)
    return out


def _stride_percentile(vol: np.ndarray, q: tuple[float, float], target_n: int = 100_000) -> np.ndarray:
    """Estimate np.percentile on a strided sample of the volume.

    For a 50x512x1024 = 26M-voxel volume, full np.percentile takes ~150-200 ms.
    Striding to ~100 k voxels takes <5 ms with negligible accuracy loss for
    1st/99th percentiles on a roughly uniform background+signal distribution.
    Sampling is deterministic (no RNG) so identical inputs always produce
    identical sidecars.
    """
    flat = vol.reshape(-1)
    stride = max(1, flat.size // target_n)
    return np.percentile(flat[::stride], list(q))


def normalize_for_3d(vol: np.ndarray, z_blur_sigma: float = 1.0) -> np.ndarray:
    """Percentile-stretch + Z-axis Gaussian blur + uint8 quantize."""
    vol = vol.astype(np.float32)
    p1, p99 = _stride_percentile(vol, (1.0, 99.0))
    vol = np.clip((vol - p1) / (p99 - p1 + 1e-8), 0, 1)
    vol = ndimage.gaussian_filter1d(vol, sigma=z_blur_sigma, axis=0)
    return (vol * 255).astype(np.uint8)
