"""Sidecar read/write roundtrip + corruption handling."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from annotator.preview_cache import (
    HEADER_SIZE,
    cache_path_for,
    is_complete,
    read_sidecar,
    write_sidecar,
)


def test_path_layout(tmp_path: Path):
    p = cache_path_for(tmp_path, "Gently", "abcd1234", "embryo_1", 7)
    assert p == tmp_path / "Gently" / "abcd1234" / "embryo_1" / "t0007.u8"


def test_roundtrip(tmp_path: Path):
    rng = np.random.default_rng(42)
    vol = rng.integers(0, 256, size=(4, 5, 6), dtype=np.uint8)
    voxel = (1.0, 0.5, 0.25)
    p = tmp_path / "out.u8"
    write_sidecar(p, vol, voxel)
    body, shape, vox = read_sidecar(p)
    assert shape == [4, 5, 6]
    assert vox == [1.0, 0.5, 0.25]
    assert body == vol.tobytes()


def test_atomic_write(tmp_path: Path):
    """A pre-existing file gets replaced; no .tmp left over."""
    vol = np.zeros((2, 2, 2), dtype=np.uint8)
    p = tmp_path / "v.u8"
    write_sidecar(p, vol, (1.0, 1.0, 1.0))
    write_sidecar(p, vol, (2.0, 2.0, 2.0))  # overwrite
    _, _, vox = read_sidecar(p)
    assert vox == [2.0, 2.0, 2.0]
    assert not (tmp_path / "v.u8.tmp").exists()


def test_bad_dtype_rejected(tmp_path: Path):
    with pytest.raises(ValueError):
        write_sidecar(tmp_path / "x.u8", np.zeros((2, 2, 2), dtype=np.uint16), (1, 1, 1))


def test_bad_shape_rejected(tmp_path: Path):
    with pytest.raises(ValueError):
        write_sidecar(tmp_path / "x.u8", np.zeros((4,), dtype=np.uint8), (1, 1, 1))


def test_truncated_body_rejected(tmp_path: Path):
    vol = np.zeros((4, 4, 4), dtype=np.uint8)
    p = tmp_path / "trunc.u8"
    write_sidecar(p, vol, (1, 1, 1))
    # Truncate the body
    full = p.read_bytes()
    p.write_bytes(full[: HEADER_SIZE + 5])
    with pytest.raises(ValueError):
        read_sidecar(p)


def test_bad_magic_rejected(tmp_path: Path):
    p = tmp_path / "garbage.u8"
    p.write_bytes(b"NOPE" + b"\x00" * 200)
    with pytest.raises(ValueError):
        read_sidecar(p)


def test_is_complete(tmp_path: Path):
    vol = np.zeros((2, 2, 2), dtype=np.uint8)
    p = tmp_path / "v.u8"
    assert not is_complete(p)
    write_sidecar(p, vol, (1, 1, 1))
    assert is_complete(p)
