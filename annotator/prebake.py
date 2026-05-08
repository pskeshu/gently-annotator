"""Background pre-bake of preview sidecars.

When a user opens an embryo we kick off a process pool that walks every
timepoint, runs the preprocess pipeline, and writes a sidecar per the
preview_cache format. After the pool drains, every fetch hits the disk
cache and returns in ~80 ms.

A single PreBakeManager instance is shared across the FastAPI app
(constructed once in lifespan). Only one embryo's bake is active at a
time — selecting a different embryo cancels pending futures and starts
a fresh batch.
"""

from __future__ import annotations

import logging
from concurrent.futures import Future, ProcessPoolExecutor
from pathlib import Path
from threading import RLock
from typing import Optional

from .preview_cache import cache_path_for, is_complete as sidecar_is_complete

logger = logging.getLogger(__name__)


def _bake_one(tif_path: str, cache_path: str, voxel_size_um: list[float]) -> str:
    """Worker entry point — pickled and sent to a child process.

    Imports happen inside so the worker module is self-contained on Windows
    spawn. Idempotent: if the sidecar already exists, no work is done.
    """
    from pathlib import Path as _Path

    from .preview_cache import is_complete as _is_complete, write_sidecar
    from .volume_io import load_volume, normalize_for_3d, preprocess

    cp = _Path(cache_path)
    if _is_complete(cp):
        return str(cp)

    vol = load_volume(_Path(tif_path))
    vol = preprocess(vol)
    vol_uint8 = normalize_for_3d(vol)
    write_sidecar(cp, vol_uint8, voxel_size_um)
    return str(cp)


class PreBakeManager:
    def __init__(
        self,
        cache_root: Path,
        voxel_size_um: list[float],
        max_workers: int = 4,
    ):
        self.cache_root = Path(cache_root)
        self.voxel_size_um = list(voxel_size_um)
        self.max_workers = max_workers
        self._executor: Optional[ProcessPoolExecutor] = None
        self._lock = RLock()

        # Per-embryo job state.
        self._embryo_key: Optional[tuple[str, str, str]] = None
        self._futures: list[Future] = []
        self._total: int = 0       # how many sidecars we set out to bake
        self._done: int = 0        # callbacks fired (success + failure)
        self._errors: int = 0
        self._already_complete: int = 0  # sidecars that already existed at start

    # --- public ---

    def start(
        self,
        dataset: str,
        session: str,
        embryo: str,
        timepoint_to_path: dict[int, Path],
    ) -> dict:
        """Cancel any existing job, then submit one bake task per missing sidecar."""
        with self._lock:
            new_key = (dataset, session, embryo)
            self._cancel_locked()
            self._ensure_executor()

            jobs: list[tuple[str, str]] = []
            already = 0
            for tp, tif_path in sorted(timepoint_to_path.items()):
                cp = cache_path_for(self.cache_root, dataset, session, embryo, tp)
                if sidecar_is_complete(cp):
                    already += 1
                    continue
                jobs.append((str(tif_path), str(cp)))

            self._embryo_key = new_key
            self._total = len(jobs)
            self._done = 0
            self._errors = 0
            self._already_complete = already
            self._futures = []

            for tif_path, cache_path in jobs:
                f = self._executor.submit(
                    _bake_one, tif_path, cache_path, self.voxel_size_um
                )
                f.add_done_callback(self._on_done)
                self._futures.append(f)

            return self._status_locked()

    def status(self, dataset: str, session: str, embryo: str) -> dict:
        with self._lock:
            if self._embryo_key != (dataset, session, embryo):
                return {
                    "embryo": (dataset, session, embryo),
                    "running": False,
                    "total": 0,
                    "done": 0,
                    "errors": 0,
                    "already_complete": 0,
                    "match": False,
                }
            return {**self._status_locked(), "match": True}

    def cancel(self) -> None:
        with self._lock:
            self._cancel_locked()

    def shutdown(self) -> None:
        with self._lock:
            self._cancel_locked()
            if self._executor is not None:
                self._executor.shutdown(wait=False, cancel_futures=True)
                self._executor = None

    # --- internals ---

    def _ensure_executor(self) -> None:
        if self._executor is None:
            self._executor = ProcessPoolExecutor(max_workers=self.max_workers)

    def _on_done(self, f: Future) -> None:
        with self._lock:
            self._done += 1
            exc = f.exception() if not f.cancelled() else None
            if exc is not None:
                self._errors += 1
                logger.warning("pre-bake task failed: %s", exc)

    def _cancel_locked(self) -> None:
        # Best-effort: futures already running can't be killed, but pending
        # ones won't start. Workers stay alive in the pool for the next batch.
        for f in self._futures:
            if not f.done():
                f.cancel()
        self._futures = []
        self._embryo_key = None
        self._total = 0
        self._done = 0
        self._errors = 0
        self._already_complete = 0

    def _status_locked(self) -> dict:
        running = any(not f.done() for f in self._futures)
        ds, ss, em = self._embryo_key if self._embryo_key else (None, None, None)
        return {
            "dataset": ds,
            "session": ss,
            "embryo": em,
            "running": running,
            "total": self._total,
            "done": self._done,
            "errors": self._errors,
            "already_complete": self._already_complete,
        }
