"""Unit tests for the volume preprocessing pipeline."""

from __future__ import annotations

import numpy as np
import pytest

from annotator.volume_io import preprocess


def _make_vol(z=4, y=8, x=16, dtype=np.int32) -> np.ndarray:
    """Synthetic volume: left half bright, right half dim."""
    vol = np.full((z, y, x), 100, dtype=dtype)  # ~background pedestal
    vol[:, :, : x // 2] += 50                   # left view: signal
    vol[:, :, x // 2 :] += 5                    # right view: faint
    return vol


def test_preprocess_left_default():
    vol = _make_vol()
    out = preprocess(vol)
    assert out.shape == (4, 8, 8)
    # left half had 100+50; subtract 100 → 50.
    assert int(out.min()) == 50
    assert int(out.max()) == 50


def test_preprocess_right():
    vol = _make_vol()
    out = preprocess(vol, view="right")
    assert out.shape == (4, 8, 8)
    # right half had 100+5; subtract 100 → 5.
    assert int(out.min()) == 5
    assert int(out.max()) == 5


def test_preprocess_clips_below_background_to_zero():
    vol = np.full((2, 4, 4), 50, dtype=np.int32)  # below 100 everywhere
    out = preprocess(vol, view="left", bg_offset=100)
    assert out.shape == (2, 4, 2)
    assert int(out.max()) == 0  # all clipped to 0
    assert int(out.min()) == 0


def test_preprocess_uint16_no_underflow():
    """uint16 below pedestal must not wrap to a huge value."""
    vol = np.full((2, 4, 4), 50, dtype=np.uint16)
    out = preprocess(vol)
    # If the subtraction had been done in uint16 we'd see ~65k here.
    assert int(out.max()) == 0


def test_preprocess_rejects_odd_x():
    vol = np.zeros((2, 4, 5), dtype=np.int32)
    with pytest.raises(ValueError):
        preprocess(vol)


def test_preprocess_rejects_non_3d():
    vol = np.zeros((4, 4), dtype=np.int32)
    with pytest.raises(ValueError):
        preprocess(vol)
