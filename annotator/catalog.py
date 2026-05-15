"""Dataset / session / embryo discovery on disk.

Schemas supported:

  v1 (Gently):  {root}/images/{session}/embryo_N_tNNNN[_YYYYMMDD_HHMMSS].tif
  v2 (Gently2): {root}/volumes/{session}/embryo_N_tNNNN.tif
  huggingface:  {root}/volumes/embryo_N/embryo_N_YYYYMMDD_HHMMSS.tif
                + {root}/volumes/embryo_N/annotations.json (session id
                lives inside the JSON; there's no per-session subdir).
                Timepoints have no `tNNNN` in the filename — index is
                derived from chronological-sorted order, 0..N-1.

Each dataset has an ordered list of candidate roots; the first one that
exists wins. So a local D: copy transparently takes precedence over a
nearline UNC path once it finishes copying, with no config change.

Discovery is lazy:
- Constructor only resolves dataset roots.
- Sessions are listed (cheap iterdir) on first request.
- Per-session embryo / timepoint scan happens on first request and is cached.
"""

from __future__ import annotations

import json
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

# HF schema: no t-number, just embryo + capture timestamp. We index by
# sorted filename — which is chronological because YYYYMMDD_HHMMSS is
# monotonic — so missing the t-number is harmless.
TIF_PATTERN_HF = re.compile(
    r"^embryo_(\d+)_(\d{8}_\d{6})\.tif$",
    re.IGNORECASE,
)

# Schema → name of the directory under the dataset root that holds session subdirs.
_SESSIONS_SUBDIR = {
    "v1": "images",
    "v2": "volumes",
    "huggingface": "volumes",
}

# Schemas that don't have a per-session subdirectory level — embryos sit
# directly under {root}/{subdir}/, and the session id is read from the
# embryo's sidecar annotations.json.
_FLAT_EMBRYO_SCHEMAS = {"huggingface"}


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
        """Session ids for the dataset.

        v1/v2: cheap iterdir of {root}/{images|volumes}/. Each subdir is
        already a session id.

        huggingface: there's no session subdir level — embryo dirs sit
        flat under {root}/volumes/. We peek into each embryo's
        annotations.json and return the distinct `session_id` values. If
        no annotations.json exists for an embryo it's still surfaced
        under a synthetic "unsessioned" bucket so the user can find it.
        """
        if dataset not in self.datasets:
            raise KeyError(dataset)
        ds = self.datasets[dataset]
        if ds.schema in _FLAT_EMBRYO_SCHEMAS:
            return self._list_sessions_hf(dataset)
        sessions_dir = self._sessions_dir(dataset)
        if sessions_dir is None or not sessions_dir.exists():
            return []
        return sorted(p.name for p in sessions_dir.iterdir() if p.is_dir())

    def _list_sessions_hf(self, dataset: str) -> list[str]:
        sessions_dir = self._sessions_dir(dataset)
        if sessions_dir is None or not sessions_dir.exists():
            return []
        sids: set[str] = set()
        for entry in sessions_dir.iterdir():
            if not entry.is_dir():
                continue
            sid = _read_session_id(entry / "annotations.json") or "unsessioned"
            sids.add(sid)
        return sorted(sids)

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

        ds = self.datasets.get(dataset)
        if ds is None:
            return None
        if ds.schema in _FLAT_EMBRYO_SCHEMAS:
            meta = self._get_session_hf(dataset, session)
        else:
            meta = self._get_session_standard(dataset, session)
        if meta is not None:
            self._session_cache[key] = meta
        return meta

    def _get_session_standard(self, dataset: str, session: str) -> SessionMeta | None:
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
        return SessionMeta(session_id=session, embryos=embryos)

    def _get_session_hf(self, dataset: str, session: str) -> SessionMeta | None:
        """HF schema: collect every embryo dir whose annotations.json
        session_id matches. Within each, sort TIFs by filename
        (chronological) and index 0..N-1.
        """
        sessions_dir = self._sessions_dir(dataset)
        if sessions_dir is None or not sessions_dir.exists():
            return None

        embryos: dict[str, EmbryoMeta] = {}
        for entry in sessions_dir.iterdir():
            if not entry.is_dir():
                continue
            sid = _read_session_id(entry / "annotations.json") or "unsessioned"
            if sid != session:
                continue
            tif_files = sorted(
                p for p in entry.iterdir()
                if p.is_file() and TIF_PATTERN_HF.match(p.name)
            )
            if not tif_files:
                continue
            # The directory name IS the embryo id (e.g. "embryo_1"). The
            # filename also encodes the embryo number; if they disagree
            # we trust the directory.
            embryo_id = entry.name
            em = EmbryoMeta(embryo_id=embryo_id)
            for idx, p in enumerate(tif_files):
                em.timepoints.append(Timepoint(timepoint=idx, path=p))
            embryos[embryo_id] = em

        if not embryos:
            return None
        return SessionMeta(session_id=session, embryos=embryos)

    def sidecar_path(self, dataset: str, embryo: str) -> Path | None:
        """Return the annotations.json path for an HF embryo, else None.

        Only HF-schema datasets have a per-embryo sidecar. Returns None
        for v1/v2 (which use the SQLite store exclusively).
        """
        ds = self.datasets.get(dataset)
        if ds is None or ds.schema not in _FLAT_EMBRYO_SCHEMAS:
            return None
        sessions_dir = self._sessions_dir(dataset)
        if sessions_dir is None:
            return None
        return sessions_dir / embryo / "annotations.json"

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


def _read_session_id(path: Path) -> str | None:
    """Read `session_id` from an HF-schema annotations.json sidecar.

    Returns None for missing/unreadable/malformed JSONs — callers fall
    back to a synthetic "unsessioned" bucket so the embryo is still
    visible in the catalog.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    sid = data.get("session_id")
    return sid if isinstance(sid, str) and sid else None
