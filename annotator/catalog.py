"""Dataset / session / embryo discovery on disk.

Schemas supported:

  v1 (Gently):  {root}/images/{session}/embryo_N_tNNNN[_YYYYMMDD_HHMMSS].tif
  v2 (Gently2): {root}/volumes/{session}/embryo_N_tNNNN.tif

Each dataset has an ordered list of candidate roots; the first one that
exists wins. So a local D: copy transparently takes precedence over a
nearline UNC path once it finishes copying, with no config change.

Discovery is lazy:
- Constructor only resolves dataset roots.
- Sessions are listed (cheap iterdir) on first request.
- Per-session embryo / timepoint scan happens on first request and is cached.
"""

from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)

# Matches both v1 (with timestamp) and v2 (no timestamp) filenames.
TIF_PATTERN = re.compile(
    r"^embryo_(\d+)_t(\d+)(?:_\d{8}_\d{6})?\.tif$",
    re.IGNORECASE,
)

# Schema → name of the directory under the dataset root that holds session subdirs.
_SESSIONS_SUBDIR = {
    "v1": "images",
    "v2": "volumes",
}


@dataclass(frozen=True)
class Timepoint:
    timepoint: int
    path: Path


@dataclass
class EmbryoMeta:
    embryo_id: str  # e.g. "embryo_1"
    timepoints: list[Timepoint] = field(default_factory=list)


@dataclass
class SessionMeta:
    session_id: str
    embryos: dict[str, EmbryoMeta] = field(default_factory=dict)

    @property
    def total_timepoints(self) -> int:
        return sum(len(e.timepoints) for e in self.embryos.values())


@dataclass
class DatasetMeta:
    name: str
    schema: str
    root: Path | None  # None if no candidate root exists yet


@dataclass(frozen=True)
class SessionSummary:
    """Compact view of a session for the sidebar — counts only, no file paths."""
    session_id: str
    embryo_count: int
    timepoint_count: int
    embryo_ids: tuple[str, ...]


class Catalog:
    def __init__(self, datasets_config: dict, stages: list[str]):
        self.datasets: dict[str, DatasetMeta] = {}
        for name, cfg in datasets_config.items():
            schema = cfg["schema"]
            if schema not in _SESSIONS_SUBDIR:
                raise ValueError(f"Dataset {name}: unknown schema {schema!r}")
            root = self._pick_root(cfg["roots"])
            self.datasets[name] = DatasetMeta(name=name, schema=schema, root=root)
            logger.info(
                "Dataset %s (schema=%s): root=%s",
                name, schema, root if root else "<none available>",
            )
        self.stages = list(stages)
        self._session_cache: dict[tuple[str, str], SessionMeta] = {}
        self._summary_cache: dict[str, list[SessionSummary]] = {}
        # One lock per dataset; serializes concurrent scans (e.g. background
        # prewarm racing the first user request) so we don't double-walk SMB.
        self._summary_locks: dict[str, threading.Lock] = {
            name: threading.Lock() for name in self.datasets
        }

    # ----- helpers -----

    @staticmethod
    def _pick_root(candidates: Iterable[str]) -> Path | None:
        for c in candidates:
            p = Path(c)
            try:
                if p.exists():
                    return p
            except OSError as e:
                logger.warning("Could not stat root %s: %s", c, e)
        return None

    def _sessions_dir(self, dataset: str) -> Path | None:
        ds = self.datasets[dataset]
        if ds.root is None:
            return None
        return ds.root / _SESSIONS_SUBDIR[ds.schema]

    # ----- public API -----

    def list_datasets(self) -> list[DatasetMeta]:
        return list(self.datasets.values())

    def list_sessions(self, dataset: str) -> list[str]:
        """Just session-dir names. Cheap; doesn't walk into them."""
        if dataset not in self.datasets:
            raise KeyError(dataset)
        sessions_dir = self._sessions_dir(dataset)
        if sessions_dir is None or not sessions_dir.exists():
            return []
        return sorted(p.name for p in sessions_dir.iterdir() if p.is_dir())

    def list_session_summaries(self, dataset: str) -> list[SessionSummary]:
        """One row per session with embryo/timepoint counts.

        Walks every session dir to populate counts. First call per dataset
        is slow (especially over SMB); cached after that. Sorted with the
        sessions that actually have data first. Concurrent calls on the
        same dataset block on a per-dataset lock so SMB only gets walked once.
        """
        if dataset in self._summary_cache:
            return self._summary_cache[dataset]

        lock = self._summary_locks.setdefault(dataset, threading.Lock())
        with lock:
            # Re-check inside the lock — another caller may have populated
            # the cache while we were waiting.
            if dataset in self._summary_cache:
                return self._summary_cache[dataset]

            out: list[SessionSummary] = []
            for sid in self.list_sessions(dataset):
                sm = self.get_session(dataset, sid)
                if sm is None:
                    continue
                out.append(
                    SessionSummary(
                        session_id=sid,
                        embryo_count=len(sm.embryos),
                        timepoint_count=sm.total_timepoints,
                        embryo_ids=tuple(sorted(sm.embryos.keys())),
                    )
                )
            # Most embryos first, then most timepoints, then session id.
            out.sort(
                key=lambda s: (-s.embryo_count, -s.timepoint_count, s.session_id)
            )
            self._summary_cache[dataset] = out
            return out

    def get_session(self, dataset: str, session: str) -> SessionMeta | None:
        key = (dataset, session)
        if key in self._session_cache:
            return self._session_cache[key]

        sessions_dir = self._sessions_dir(dataset)
        if sessions_dir is None:
            return None
        session_dir = sessions_dir / session
        if not session_dir.exists() or not session_dir.is_dir():
            return None

        embryos: dict[str, EmbryoMeta] = {}
        for entry in session_dir.iterdir():
            if not entry.is_file():
                continue
            m = TIF_PATTERN.match(entry.name)
            if not m:
                continue
            embryo_n = int(m.group(1))
            tp_n = int(m.group(2))
            embryo_id = f"embryo_{embryo_n}"
            embryos.setdefault(
                embryo_id, EmbryoMeta(embryo_id=embryo_id)
            ).timepoints.append(Timepoint(timepoint=tp_n, path=entry))

        for emb in embryos.values():
            emb.timepoints.sort(key=lambda t: t.timepoint)

        meta = SessionMeta(session_id=session, embryos=embryos)
        self._session_cache[key] = meta
        return meta

    def get_timepoint_path(
        self, dataset: str, session: str, embryo: str, timepoint: int
    ) -> Path | None:
        sm = self.get_session(dataset, session)
        if sm is None:
            return None
        em = sm.embryos.get(embryo)
        if em is None:
            return None
        for tp in em.timepoints:
            if tp.timepoint == timepoint:
                return tp.path
        return None

    def invalidate(self) -> None:
        """Drop the per-session and per-dataset caches (use after the disk layout changes)."""
        self._session_cache.clear()
        self._summary_cache.clear()
