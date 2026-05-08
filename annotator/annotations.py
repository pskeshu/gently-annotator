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

    # ---- summary across all of one annotator's work ----

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
