"""SQLite store for transition markers, per-timepoint notes, and per-embryo flags.

Annotation model:
- Stage transitions: annotators mark the *first timepoint that looks like* each
  stage. The stage at any given timepoint is then the latest transition
  at-or-before it (per annotator). C. elegans embryogenesis is monotonic
  enough for this to work.
- Notes: free-text descriptions of visual morphology, one per (annotator,
  embryo, timepoint). Optional and orthogonal to stage labels — a note can
  flag uncertainty, describe a feature, or just be working memory.
- Exclude flag: per-embryo opt-out (out-of-focus, drifted, dead).

Each annotator owns their own rows for each kind of annotation, so multiple
people can label the same embryo without merge conflicts.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS transitions (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset     TEXT    NOT NULL,
  session     TEXT    NOT NULL,
  embryo      TEXT    NOT NULL,
  stage       TEXT    NOT NULL,
  timepoint   INTEGER NOT NULL,
  annotator   TEXT    NOT NULL,
  notes       TEXT,
  updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(dataset, session, embryo, stage, annotator)
);
CREATE INDEX IF NOT EXISTS idx_transitions_lookup
  ON transitions(dataset, session, embryo, annotator);

CREATE TABLE IF NOT EXISTS embryo_flags (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset     TEXT    NOT NULL,
  session     TEXT    NOT NULL,
  embryo      TEXT    NOT NULL,
  excluded    INTEGER NOT NULL DEFAULT 0,
  notes       TEXT,
  annotator   TEXT    NOT NULL,
  updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(dataset, session, embryo, annotator)
);

CREATE TABLE IF NOT EXISTS timepoint_notes (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset     TEXT    NOT NULL,
  session     TEXT    NOT NULL,
  embryo      TEXT    NOT NULL,
  timepoint   INTEGER NOT NULL,
  note        TEXT    NOT NULL,
  annotator   TEXT    NOT NULL,
  updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(dataset, session, embryo, timepoint, annotator)
);
CREATE INDEX IF NOT EXISTS idx_timepoint_notes_lookup
  ON timepoint_notes(dataset, session, embryo, annotator);

-- Per-timepoint body-axis orientation (anterior-posterior, dorsal-ventral).
-- ap_dir / dv_dir are JSON [x, y, z] unit vectors in volume-local
-- coordinates. Either may be NULL — annotators can save one axis
-- without the other.
CREATE TABLE IF NOT EXISTS orientations (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset     TEXT    NOT NULL,
  session     TEXT    NOT NULL,
  embryo      TEXT    NOT NULL,
  timepoint   INTEGER NOT NULL,
  ap_dir      TEXT,
  dv_dir      TEXT,
  annotator   TEXT    NOT NULL,
  notes       TEXT,
  updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(dataset, session, embryo, timepoint, annotator)
);
CREATE INDEX IF NOT EXISTS idx_orientations_lookup
  ON orientations(dataset, session, embryo, annotator);

-- Closed [start_tp, end_tp] ranges where the annotator declared the
-- orientation unreliable (twitching, ambiguity, occlusion, etc.).
-- "View notes" — annotations attached to a specific reproducible camera
-- pose. Many allowed per (dataset, session, embryo, timepoint, annotator)
-- since each one labels a distinct view. view_params is a JSON blob
-- containing rotation_quat (4 floats), zoom (1 float), threshold (0–100),
-- contrast (0.5–3.0), and a version field for future-proofing. tag is an
-- optional free-form short string (e.g. "best", "worst", "occluded").
CREATE TABLE IF NOT EXISTS view_notes (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset     TEXT    NOT NULL,
  session     TEXT    NOT NULL,
  embryo      TEXT    NOT NULL,
  timepoint   INTEGER NOT NULL,
  view_params TEXT    NOT NULL,
  note        TEXT    NOT NULL,
  tag         TEXT,
  annotator   TEXT    NOT NULL,
  updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_view_notes_lookup
  ON view_notes(dataset, session, embryo, annotator);
CREATE INDEX IF NOT EXISTS idx_view_notes_tp
  ON view_notes(dataset, session, embryo, timepoint, annotator);

CREATE TABLE IF NOT EXISTS orientation_unreliable_ranges (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset     TEXT    NOT NULL,
  session     TEXT    NOT NULL,
  embryo      TEXT    NOT NULL,
  start_tp    INTEGER NOT NULL,
  end_tp      INTEGER NOT NULL,
  annotator   TEXT    NOT NULL,
  notes       TEXT,
  updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CHECK (end_tp >= start_tp)
);
CREATE INDEX IF NOT EXISTS idx_orient_unreliable_lookup
  ON orientation_unreliable_ranges(dataset, session, embryo, annotator);
"""


class AnnotationStore:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.executescript(_SCHEMA)

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    # ---- transitions ----

    def upsert_transition(
        self,
        dataset: str,
        session: str,
        embryo: str,
        stage: str,
        timepoint: int,
        annotator: str,
        notes: str | None = None,
    ) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO transitions
                  (dataset, session, embryo, stage, timepoint, annotator, notes, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(dataset, session, embryo, stage, annotator)
                DO UPDATE SET
                  timepoint  = excluded.timepoint,
                  notes      = excluded.notes,
                  updated_at = CURRENT_TIMESTAMP
                """,
                (dataset, session, embryo, stage, timepoint, annotator, notes),
            )

    def list_transitions(
        self,
        dataset: str,
        session: str,
        embryo: str,
        annotator: str | None = None,
    ) -> list[dict]:
        with self._conn() as c:
            if annotator is not None:
                rows = c.execute(
                    """
                    SELECT * FROM transitions
                    WHERE dataset=? AND session=? AND embryo=? AND annotator=?
                    ORDER BY timepoint
                    """,
                    (dataset, session, embryo, annotator),
                ).fetchall()
            else:
                rows = c.execute(
                    """
                    SELECT * FROM transitions
                    WHERE dataset=? AND session=? AND embryo=?
                    ORDER BY annotator, timepoint
                    """,
                    (dataset, session, embryo),
                ).fetchall()
            return [dict(r) for r in rows]

    def delete_transition(
        self, dataset: str, session: str, embryo: str, stage: str, annotator: str
    ) -> None:
        with self._conn() as c:
            c.execute(
                """
                DELETE FROM transitions
                WHERE dataset=? AND session=? AND embryo=? AND stage=? AND annotator=?
                """,
                (dataset, session, embryo, stage, annotator),
            )

    # ---- flags ----

    def upsert_flag(
        self,
        dataset: str,
        session: str,
        embryo: str,
        annotator: str,
        excluded: bool,
        notes: str | None = None,
    ) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO embryo_flags
                  (dataset, session, embryo, excluded, notes, annotator, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(dataset, session, embryo, annotator)
                DO UPDATE SET
                  excluded   = excluded.excluded,
                  notes      = excluded.notes,
                  updated_at = CURRENT_TIMESTAMP
                """,
                (dataset, session, embryo, int(excluded), notes, annotator),
            )

    def get_flag(
        self, dataset: str, session: str, embryo: str, annotator: str
    ) -> dict | None:
        with self._conn() as c:
            row = c.execute(
                """
                SELECT * FROM embryo_flags
                WHERE dataset=? AND session=? AND embryo=? AND annotator=?
                """,
                (dataset, session, embryo, annotator),
            ).fetchone()
            return dict(row) if row else None

    # ---- timepoint notes ----

    def upsert_note(
        self,
        dataset: str,
        session: str,
        embryo: str,
        timepoint: int,
        annotator: str,
        note: str,
    ) -> None:
        """Save a free-text note. Empty/whitespace-only deletes instead."""
        if not note or not note.strip():
            self.delete_note(dataset, session, embryo, timepoint, annotator)
            return
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO timepoint_notes
                  (dataset, session, embryo, timepoint, note, annotator, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(dataset, session, embryo, timepoint, annotator)
                DO UPDATE SET
                  note       = excluded.note,
                  updated_at = CURRENT_TIMESTAMP
                """,
                (dataset, session, embryo, timepoint, note, annotator),
            )

    def list_notes(
        self,
        dataset: str,
        session: str,
        embryo: str,
        annotator: str,
    ) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT * FROM timepoint_notes
                WHERE dataset=? AND session=? AND embryo=? AND annotator=?
                ORDER BY timepoint
                """,
                (dataset, session, embryo, annotator),
            ).fetchall()
            return [dict(r) for r in rows]

    def delete_note(
        self,
        dataset: str,
        session: str,
        embryo: str,
        timepoint: int,
        annotator: str,
    ) -> None:
        with self._conn() as c:
            c.execute(
                """
                DELETE FROM timepoint_notes
                WHERE dataset=? AND session=? AND embryo=?
                  AND timepoint=? AND annotator=?
                """,
                (dataset, session, embryo, timepoint, annotator),
            )

    # ---- orientation: AP / DV per-timepoint ----

    def upsert_orientation_axis(
        self,
        dataset: str,
        session: str,
        embryo: str,
        timepoint: int,
        annotator: str,
        axis: str,
        direction: list[float] | None,
    ) -> None:
        """Set or clear ONE axis (ap or dv) at a timepoint.

        If `direction` is None the column is set to NULL. If both columns end
        up NULL (and notes is empty) we delete the row to keep the table clean.
        """
        if axis not in ("ap", "dv"):
            raise ValueError(f"axis must be 'ap' or 'dv', got {axis!r}")
        col = "ap_dir" if axis == "ap" else "dv_dir"
        import json as _json
        value = _json.dumps(direction) if direction is not None else None

        with self._conn() as c:
            c.execute(
                f"""
                INSERT INTO orientations
                  (dataset, session, embryo, timepoint, annotator, {col}, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(dataset, session, embryo, timepoint, annotator)
                DO UPDATE SET
                  {col}      = excluded.{col},
                  updated_at = CURRENT_TIMESTAMP
                """,
                (dataset, session, embryo, timepoint, annotator, value),
            )
            # Garbage-collect rows that no longer carry data.
            c.execute(
                """
                DELETE FROM orientations
                WHERE dataset=? AND session=? AND embryo=? AND timepoint=? AND annotator=?
                  AND ap_dir IS NULL AND dv_dir IS NULL
                  AND (notes IS NULL OR notes = '')
                """,
                (dataset, session, embryo, timepoint, annotator),
            )

    def list_orientations(
        self,
        dataset: str,
        session: str,
        embryo: str,
        annotator: str,
    ) -> list[dict]:
        import json as _json
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT * FROM orientations
                WHERE dataset=? AND session=? AND embryo=? AND annotator=?
                ORDER BY timepoint
                """,
                (dataset, session, embryo, annotator),
            ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["ap_dir"] = _json.loads(d["ap_dir"]) if d["ap_dir"] else None
            d["dv_dir"] = _json.loads(d["dv_dir"]) if d["dv_dir"] else None
            out.append(d)
        return out

    def clear_orientation(
        self,
        dataset: str,
        session: str,
        embryo: str,
        timepoint: int,
        annotator: str,
    ) -> None:
        with self._conn() as c:
            c.execute(
                """
                DELETE FROM orientations
                WHERE dataset=? AND session=? AND embryo=? AND timepoint=? AND annotator=?
                """,
                (dataset, session, embryo, timepoint, annotator),
            )

    # ---- orientation: unreliable ranges ----

    def add_unreliable_range(
        self,
        dataset: str,
        session: str,
        embryo: str,
        start_tp: int,
        end_tp: int,
        annotator: str,
        notes: str | None = None,
    ) -> int:
        if end_tp < start_tp:
            start_tp, end_tp = end_tp, start_tp
        with self._conn() as c:
            cur = c.execute(
                """
                INSERT INTO orientation_unreliable_ranges
                  (dataset, session, embryo, start_tp, end_tp, annotator, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (dataset, session, embryo, start_tp, end_tp, annotator, notes),
            )
            return cur.lastrowid

    def list_unreliable_ranges(
        self,
        dataset: str,
        session: str,
        embryo: str,
        annotator: str,
    ) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT * FROM orientation_unreliable_ranges
                WHERE dataset=? AND session=? AND embryo=? AND annotator=?
                ORDER BY start_tp
                """,
                (dataset, session, embryo, annotator),
            ).fetchall()
            return [dict(r) for r in rows]

    def delete_unreliable_range(self, range_id: int, annotator: str) -> None:
        with self._conn() as c:
            c.execute(
                "DELETE FROM orientation_unreliable_ranges WHERE id=? AND annotator=?",
                (range_id, annotator),
            )

    # ---- view notes ----

    def add_view_note(
        self,
        dataset: str,
        session: str,
        embryo: str,
        timepoint: int,
        annotator: str,
        view_params: dict,
        note: str,
        tag: str | None = None,
    ) -> int:
        import json as _json
        with self._conn() as c:
            cur = c.execute(
                """
                INSERT INTO view_notes
                  (dataset, session, embryo, timepoint, view_params, note, tag, annotator)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    dataset, session, embryo, timepoint,
                    _json.dumps(view_params), note, tag, annotator,
                ),
            )
            return cur.lastrowid

    def update_view_note(
        self,
        note_id: int,
        annotator: str,
        note: str | None = None,
        tag: str | None = None,
        view_params: dict | None = None,
    ) -> None:
        """Patch any subset of (note, tag, view_params). Annotator owns the row."""
        import json as _json
        sets = []
        params: list = []
        if note is not None:
            sets.append("note = ?")
            params.append(note)
        if tag is not None:
            sets.append("tag = ?")
            params.append(tag)
        if view_params is not None:
            sets.append("view_params = ?")
            params.append(_json.dumps(view_params))
        if not sets:
            return
        sets.append("updated_at = CURRENT_TIMESTAMP")
        params.extend([note_id, annotator])
        with self._conn() as c:
            c.execute(
                f"UPDATE view_notes SET {', '.join(sets)} "
                f"WHERE id = ? AND annotator = ?",
                params,
            )

    def list_view_notes(
        self,
        dataset: str,
        session: str,
        embryo: str,
        annotator: str,
    ) -> list[dict]:
        import json as _json
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT * FROM view_notes
                WHERE dataset=? AND session=? AND embryo=? AND annotator=?
                ORDER BY timepoint, id
                """,
                (dataset, session, embryo, annotator),
            ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["view_params"] = _json.loads(d["view_params"])
            except Exception:
                d["view_params"] = {}
            out.append(d)
        return out

    def delete_view_note(self, note_id: int, annotator: str) -> None:
        with self._conn() as c:
            c.execute(
                "DELETE FROM view_notes WHERE id=? AND annotator=?",
                (note_id, annotator),
            )

    # ---- summary across all of one annotator's work ----

    # ---- known annotators across the whole DB ----

    def list_known_annotators(self) -> list[str]:
        """Distinct annotator names that have at least one row in any table.

        Used to populate the "view as someone else" dropdown so users can
        pick from people who've actually done work, not type a free-form
        name and risk a typo.
        """
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT annotator FROM transitions
                UNION SELECT annotator FROM timepoint_notes
                UNION SELECT annotator FROM embryo_flags
                UNION SELECT annotator FROM orientations
                UNION SELECT annotator FROM orientation_unreliable_ranges
                ORDER BY annotator COLLATE NOCASE
                """
            ).fetchall()
            return [r[0] for r in rows]

    def summary_for_annotator(self, annotator: str) -> list[dict]:
        """One row per (dataset, session, embryo) where the annotator has any data.

        Used to highlight the sidebar — keeps it cheap (three small GROUP BYs).
        """
        with self._conn() as c:
            t_rows = c.execute(
                """
                SELECT dataset, session, embryo, COUNT(*) AS n
                FROM transitions
                WHERE annotator=?
                GROUP BY dataset, session, embryo
                """,
                (annotator,),
            ).fetchall()
            n_rows = c.execute(
                """
                SELECT dataset, session, embryo, COUNT(*) AS n
                FROM timepoint_notes
                WHERE annotator=?
                GROUP BY dataset, session, embryo
                """,
                (annotator,),
            ).fetchall()
            f_rows = c.execute(
                """
                SELECT dataset, session, embryo, excluded
                FROM embryo_flags
                WHERE annotator=?
                """,
                (annotator,),
            ).fetchall()

        merged: dict[tuple[str, str, str], dict] = {}
        for r in t_rows:
            key = (r["dataset"], r["session"], r["embryo"])
            merged.setdefault(key, {"transitions": 0, "notes": 0, "excluded": False})
            merged[key]["transitions"] = r["n"]
        for r in n_rows:
            key = (r["dataset"], r["session"], r["embryo"])
            merged.setdefault(key, {"transitions": 0, "notes": 0, "excluded": False})
            merged[key]["notes"] = r["n"]
        for r in f_rows:
            key = (r["dataset"], r["session"], r["embryo"])
            merged.setdefault(key, {"transitions": 0, "notes": 0, "excluded": False})
            merged[key]["excluded"] = bool(r["excluded"])

        return [
            {
                "dataset": ds,
                "session": ss,
                "embryo": em,
                "transitions": v["transitions"],
                "notes": v["notes"],
                "excluded": v["excluded"],
            }
            for (ds, ss, em), v in sorted(merged.items())
        ]
