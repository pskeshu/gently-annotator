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


def _signal_percentile(
    vol: np.ndarray,
    q: tuple[float, float],
    target_n: int = 100_000,
) -> np.ndarray:
    """Estimate np.percentile over the volume's *signal* voxels — the ones
    that survived the background-subtract step (i.e., > 0).

    The background subtraction in preprocess() clips below-pedestal voxels
    to zero. Including those in the percentile computation drags p99 down
    because they dominate the histogram (often >50% of voxels in dim
    Gently2 timepoints), which compresses the dynamic range and makes
    the remaining faint background noise stretch up into visibility.
    Computing percentile only on non-zero voxels gives a stretch that
    tracks the actual signal.

    For Gently1 (dense signal) this matters very little; for Gently2
    (sparse signal) it's the difference between a clean black background
    and a noisy gray haze.

    Strided sampling for speed: full np.percentile on 26M voxels is
    ~150-200 ms; sampling to ~100k voxels is <5 ms. Sampling is
    deterministic (no RNG) so identical inputs always produce identical
    sidecars.
    """
    flat = vol.reshape(-1)
    positive = flat[flat > 0]
    # Degenerate case (volume is entirely background after subtract):
    # return [0, 1] so we don't divide by zero downstream.
    if positive.size < 100:
        return np.array([0.0, 1.0], dtype=np.float64)
    stride = max(1, positive.size // target_n)
    return np.percentile(positive[::stride], list(q))


def normalize_for_3d(vol: np.ndarray, z_blur_sigma: float = 1.0) -> np.ndarray:
    """Percentile-stretch + Z-axis Gaussian blur + uint8 quantize.

    The Z-blur was added to hide diSPIM Z-banding in the raw view. Pass
    `z_blur_sigma=0` for sessions whose volumes are already de-banded
    (e.g. DeAbe-processed) — see `z_blur_sigma_for_session`.
    """
    vol = vol.astype(np.float32)
    p1, p99 = _signal_percentile(vol, (1.0, 99.0))
    vol = np.clip((vol - p1) / (p99 - p1 + 1e-8), 0, 1)
    if z_blur_sigma > 0:
        vol = ndimage.gaussian_filter1d(vol, sigma=z_blur_sigma, axis=0)
    return (vol * 255).astype(np.uint8)


def z_blur_sigma_for_session(session: str) -> float:
    """Sessions whose names end with `_deabe` skip the renderer Z-blur — DeAbe
    already removed the Z-banding artifact the blur was hiding, and the blur
    smooths over the deconvolution improvement that was the whole point."""
    return 0.0 if session.endswith("_deabe") else 1.0
