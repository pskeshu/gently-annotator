/* Tiny fetch wrappers. */
const API = {
  async datasets() {
    const r = await fetch("/api/datasets");
    if (!r.ok) throw new Error(`datasets: ${r.status}`);
    return r.json();
  },

  async sessions(dataset, { includeEmpty = false } = {}) {
    const url = `/api/datasets/${encodeURIComponent(dataset)}/sessions${includeEmpty ? "?include_empty=true" : ""}`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`sessions: ${r.status}`);
    return r.json();
  },

  async embryos(dataset, session) {
    const url = `/api/datasets/${encodeURIComponent(dataset)}/sessions/${encodeURIComponent(session)}/embryos`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`embryos: ${r.status}`);
    return r.json();
  },

  async timepoints(dataset, session, embryo) {
    const url = `/api/datasets/${encodeURIComponent(dataset)}/sessions/${encodeURIComponent(session)}/embryos/${encodeURIComponent(embryo)}/timepoints`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`timepoints: ${r.status}`);
    return r.json();
  },

  /** Fetch a preprocessed uint8 volume as raw bytes. Shape + voxel size
   *  arrive in response headers; payload is the bare voxel array. */
  async volume(dataset, session, embryo, timepoint, { signal } = {}) {
    const url = `/api/datasets/${encodeURIComponent(dataset)}/sessions/${encodeURIComponent(session)}/embryos/${encodeURIComponent(embryo)}/volumes/${timepoint}`;
    const r = await fetch(url, { signal });
    if (!r.ok) throw new Error(`volume: ${r.status}`);
    const buf = await r.arrayBuffer();
    const shape = (r.headers.get("X-Volume-Shape") || "").split(",").map(Number);
    const voxelSize = (r.headers.get("X-Volume-Voxel-Size-Um") || "1,1,1")
      .split(",")
      .map(Number);
    return {
      dataset,
      session,
      embryo,
      timepoint,
      shape,
      voxel_size_um: voxelSize,
      data: new Uint8Array(buf),
    };
  },

  // ---- annotations ----

  _annPath(dataset, session, embryo) {
    return `/api/annotations/${encodeURIComponent(dataset)}/${encodeURIComponent(session)}/${encodeURIComponent(embryo)}`;
  },

  async annotations(dataset, session, embryo, annotator) {
    const url = `${this._annPath(dataset, session, embryo)}?annotator=${encodeURIComponent(annotator)}`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`annotations: ${r.status}`);
    return r.json();
  },

  async annotationSummary(annotator) {
    const r = await fetch(`/api/annotations/summary?annotator=${encodeURIComponent(annotator)}`);
    if (!r.ok) throw new Error(`annotationSummary: ${r.status}`);
    return r.json();
  },

  async upsertTransition(dataset, session, embryo, { annotator, stage, timepoint, notes = null }) {
    const r = await fetch(`${this._annPath(dataset, session, embryo)}/transitions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ annotator, stage, timepoint, notes }),
    });
    if (!r.ok) throw new Error(`upsertTransition: ${r.status}`);
    return r.json();
  },

  async deleteTransition(dataset, session, embryo, stage, annotator) {
    const r = await fetch(
      `${this._annPath(dataset, session, embryo)}/transitions/${encodeURIComponent(stage)}?annotator=${encodeURIComponent(annotator)}`,
      { method: "DELETE" }
    );
    if (!r.ok) throw new Error(`deleteTransition: ${r.status}`);
    return r.json();
  },

  async upsertNote(dataset, session, embryo, timepoint, { annotator, note }) {
    const r = await fetch(`${this._annPath(dataset, session, embryo)}/notes/${timepoint}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ annotator, note }),
    });
    if (!r.ok) throw new Error(`upsertNote: ${r.status}`);
    return r.json();
  },

  // ---- prebake ----

  async prebakeStart(dataset, session, embryo) {
    const r = await fetch("/api/prebake/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ dataset, session, embryo }),
    });
    if (!r.ok) throw new Error(`prebakeStart: ${r.status}`);
    return r.json();
  },

  async prebakeStatus(dataset, session, embryo) {
    const url = `/api/prebake/status?dataset=${encodeURIComponent(dataset)}&session=${encodeURIComponent(session)}&embryo=${encodeURIComponent(embryo)}`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`prebakeStatus: ${r.status}`);
    return r.json();
  },

  async prebakeCancel() {
    const r = await fetch("/api/prebake/cancel", { method: "POST" });
    if (!r.ok) throw new Error(`prebakeCancel: ${r.status}`);
    return r.json();
  },

  async upsertFlag(dataset, session, embryo, { annotator, excluded, notes = null }) {
    const r = await fetch(`${this._annPath(dataset, session, embryo)}/flag`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ annotator, excluded, notes }),
    });
    if (!r.ok) throw new Error(`upsertFlag: ${r.status}`);
    return r.json();
  },
};

window.API = API;
