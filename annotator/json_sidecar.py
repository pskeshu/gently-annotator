"""Read/write annotations.json sidecars for HuggingFace-schema embryos.

The HF release ships one annotations.json per embryo directory, holding
ground-truth `stage_transitions` plus metadata. We treat that file as:

- READ ONLY for the original fields (`stage_transitions`, `annotator`,
  `annotation_date`, `total_timepoints`, `volume_shape`, `notes`,
  `session_id`, `embryo_id`). The annotator never overwrites Ryan's
  ground truth via the UI.
- READ/WRITE for a new `events` block, keyed by event name then by
  annotator. The first event we support is `twitching_start`, which is
  a single timepoint per annotator.

Schema after the first twitching write:

    {
      ...original fields untouched...,
      "events": {
        "twitching_start": {
          "Alice": {"timepoint": 87, "updated_at": "2026-05-15T10:23:00Z"},
          "Bob":   {"timepoint": 92, "updated_at": "2026-05-16T14:05:11Z"}
        }
      }
    }

Writes are atomic: stage to <path>.tmp, then os.replace.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def read_sidecar(path: Path | str) -> dict[str, Any]:
    """Return the parsed JSON. Missing/unreadable file returns {}."""
    p = Path(path)
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Could not read sidecar %s: %s", p, e)
        return {}
    return data if isinstance(data, dict) else {}


def ground_truth_transitions(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert `stage_transitions` to a list-of-dicts shape that matches
    the SQLite store's `list_transitions` output. The annotator field
    here is whoever owns the JSON (e.g. "Ryan"); the timeline renders
    these as a read-only overlay distinct from the active user's labels.
    """
    transitions = data.get("stage_transitions") or {}
    if not isinstance(transitions, dict):
        return []
    annotator = data.get("annotator") or "ground-truth"
    out: list[dict[str, Any]] = []
    for stage, tp in transitions.items():
        if not isinstance(tp, int):
            continue
        out.append(
            {
                "stage": str(stage),
                "timepoint": tp,
                "annotator": annotator,
                "notes": None,
            }
        )
    out.sort(key=lambda r: r["timepoint"])
    return out


def twitching_event(data: dict[str, Any], annotator: str) -> dict[str, Any] | None:
    """Return {timepoint, updated_at} for one annotator, or None."""
    events = (data.get("events") or {}).get("twitching_start") or {}
    if not isinstance(events, dict):
        return None
    rec = events.get(annotator)
    if not isinstance(rec, dict):
        return None
    tp = rec.get("timepoint")
    if not isinstance(tp, int):
        return None
    return {
        "annotator": annotator,
        "timepoint": tp,
        "updated_at": rec.get("updated_at"),
    }


def all_twitching_events(data: dict[str, Any]) -> list[dict[str, Any]]:
    """All twitching_start records across all annotators."""
    events = (data.get("events") or {}).get("twitching_start") or {}
    out: list[dict[str, Any]] = []
    if isinstance(events, dict):
        for name, rec in events.items():
            if isinstance(rec, dict) and isinstance(rec.get("timepoint"), int):
                out.append(
                    {
                        "annotator": name,
                        "timepoint": rec["timepoint"],
                        "updated_at": rec.get("updated_at"),
                    }
                )
    out.sort(key=lambda r: (r["annotator"], r["timepoint"]))
    return out


def upsert_twitching(path: Path | str, annotator: str, timepoint: int) -> None:
    """Set this annotator's twitching_start timepoint, preserving every
    other field in the file. Creates `events.twitching_start` if absent.
    """
    p = Path(path)
    data = read_sidecar(p)
    events = data.setdefault("events", {})
    if not isinstance(events, dict):
        events = {}
        data["events"] = events
    twitch = events.setdefault("twitching_start", {})
    if not isinstance(twitch, dict):
        twitch = {}
        events["twitching_start"] = twitch
    twitch[annotator] = {
        "timepoint": int(timepoint),
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    _atomic_write(p, data)


def delete_twitching(path: Path | str, annotator: str) -> None:
    """Remove this annotator's twitching_start. No-op if not present."""
    p = Path(path)
    data = read_sidecar(p)
    events = data.get("events") or {}
    twitch = events.get("twitching_start") or {}
    if annotator in twitch:
        del twitch[annotator]
        # Garbage-collect empty containers so the file stays tidy.
        if not twitch:
            events.pop("twitching_start", None)
        if not events:
            data.pop("events", None)
        _atomic_write(p, data)


def _atomic_write(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, path)
