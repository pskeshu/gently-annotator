/* App orchestrator — sidebar navigation + viewer wiring + annotation. */

(function () {
  const ANNOTATOR_KEY = "annotator_name";

  // Stage palette — colorblind-friendly bright colors over dark bg.
  const STAGE_COLORS = {
    "early":   "#4477AA",
    "bean":    "#66CCEE",
    "comma":   "#44AA77",
    "1.5fold": "#CCBB44",
    "2fold":   "#EE7733",
    "pretzel": "#CC3311",
    "hatched": "#AA4499",
  };

  const NOTE_DEBOUNCE_MS = 800;

  const state = {
    datasets: [],
    stages: [],
    selected: { dataset: null, session: null, embryo: null, timepoint: null },
    timepoints: [],            // timepoints for the currently selected embryo
    annotator: null,
    viewer: null,
    expanded: { datasets: new Set(), sessions: new Set() }, // "ds" / "ds/sess"
    sessionEmbryos: new Map(), // "ds/sess" → embryo[]
    sessionLists: new Map(),   // ds → {sessions, total_sessions, shown, hidden_empty}
    sessionLoading: new Set(), // datasets currently being scanned
    showEmpty: false,
    editingAnnotator: false,
    justSavedAnnotator: false,
    loadingVolume: false,
    // Annotations for the currently selected embryo (current annotator only).
    annotations: {
      loaded: false,
      transitions: [],          // [{stage, timepoint, ...}]
      notes: new Map(),         // tp → note text
      flag: { excluded: false, notes: null },
    },
    // Summary across ALL of the current annotator's work — drives sidebar
    // badges. Keyed by "dataset/session/embryo" → {transitions, notes, excluded}.
    annotationSummary: new Map(),
    noteSaveTimer: null,
    noteSaveInflight: false,
  };

  function summaryKey(dataset, session, embryo) {
    return `${dataset}/${session}/${embryo}`;
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    }[c]));
  }

  // ----- bootstrap -----

  document.addEventListener("DOMContentLoaded", async () => {
    setupAnnotator();
    setupControls();
    setupViewer();
    await loadCatalog();
  });

  function setupAnnotator() {
    const saved = (localStorage.getItem(ANNOTATOR_KEY) || "").trim();
    state.annotator = saved || null;
    state.editingAnnotator = !state.annotator;  // edit mode if no name yet
    renderAnnotator();
  }

  function renderAnnotator() {
    const root = document.getElementById("annotator-section");
    if (state.editingAnnotator || !state.annotator) {
      root.innerHTML = `
        <label for="annotator-input">You:</label>
        <input id="annotator-input" type="text" placeholder="your name" autocomplete="off" />
        <button id="annotator-save" type="button">Save</button>
      `;
      const input = root.querySelector("#annotator-input");
      const saveBtn = root.querySelector("#annotator-save");
      input.value = state.annotator || "";
      setTimeout(() => input.focus(), 0);

      const commit = () => {
        const name = input.value.trim();
        if (!name) {
          input.focus();
          return;
        }
        const changed = name !== state.annotator;
        state.annotator = name;
        state.editingAnnotator = false;
        state.justSavedAnnotator = true;
        localStorage.setItem(ANNOTATOR_KEY, name);
        renderAnnotator();
        // If the active annotator changed, refresh both the per-embryo
        // annotation set (if an embryo is loaded) and the global summary.
        if (changed) {
          flushNoteSave();
          if (state.selected.embryo) loadAnnotations();
          refreshAnnotationSummary();
        }
        setTimeout(() => {
          state.justSavedAnnotator = false;
          renderAnnotator();
        }, 1400);
      };

      saveBtn.addEventListener("click", commit);
      input.addEventListener("keydown", (e) => {
        if (e.key === "Enter") { e.preventDefault(); commit(); }
        if (e.key === "Escape" && state.annotator) {
          e.preventDefault();
          state.editingAnnotator = false;
          renderAnnotator();
        }
      });
      // Auto-save when the user tabs/clicks away with a non-empty value.
      input.addEventListener("blur", () => {
        if (input.value.trim() && input.value.trim() !== state.annotator) {
          commit();
        }
      });
    } else {
      const flash = state.justSavedAnnotator ? " saved" : "";
      root.innerHTML = `
        <span class="annotator-label">Signed in as</span>
        <span class="annotator-chip${flash}">${escapeHtml(state.annotator)}</span>
        <button id="annotator-change" type="button" class="link-btn">change</button>
      `;
      root.querySelector("#annotator-change").addEventListener("click", () => {
        state.editingAnnotator = true;
        renderAnnotator();
      });
    }
  }

  function setupControls() {
    const threshSlider = document.getElementById("threshold");
    const threshDisplay = document.getElementById("threshold-val");
    threshSlider.addEventListener("input", (e) => {
      const t = parseInt(e.target.value);
      threshDisplay.textContent = (t / 100).toFixed(2);
      if (state.viewer) state.viewer.setThreshold(t);
    });

    const contrastSlider = document.getElementById("contrast");
    const contrastDisplay = document.getElementById("contrast-val");
    contrastSlider.addEventListener("input", (e) => {
      const c = parseInt(e.target.value) / 100;
      contrastDisplay.textContent = c.toFixed(1);
      if (state.viewer) state.viewer.setContrast(c);
    });

    document.getElementById("prev-tp").addEventListener("click", () => stepTimepoint(-1));
    document.getElementById("next-tp").addEventListener("click", () => stepTimepoint(1));

    // Notes textarea — auto-save on idle and on blur.
    const ta = document.getElementById("notes-textarea");
    ta.addEventListener("input", () => scheduleNoteSave());
    ta.addEventListener("blur", () => flushNoteSave());

    // Exclude checkbox.
    document.getElementById("exclude-input").addEventListener("change", (e) => {
      toggleExclude(e.target.checked);
    });

    // Timeline strip click → jump to that timepoint.
    document.getElementById("timeline-strip").addEventListener("click", (e) => {
      if (!state.timepoints.length) return;
      const strip = e.currentTarget;
      const rect = strip.getBoundingClientRect();
      const frac = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
      const i = Math.round(frac * (state.timepoints.length - 1));
      const tp = state.timepoints[i];
      if (tp != null && tp !== state.selected.timepoint) loadTimepoint(tp);
    });

    // Hotkeys. Skip when typing in input/textarea.
    document.addEventListener("keydown", (e) => {
      const tag = e.target.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA") return;
      if (e.ctrlKey || e.metaKey || e.altKey) return;

      if (e.key === "ArrowLeft")  { stepTimepoint(-1); e.preventDefault(); return; }
      if (e.key === "ArrowRight") { stepTimepoint(1);  e.preventDefault(); return; }
      // 1..7 mark the corresponding stage at the current timepoint.
      if (/^[1-7]$/.test(e.key)) {
        const idx = parseInt(e.key, 10) - 1;
        const stage = state.stages[idx];
        if (stage) { markStage(stage); e.preventDefault(); }
        return;
      }
      if (e.key === "Backspace" || e.key === "Delete") {
        clearMarkerAtCurrentTp(); e.preventDefault(); return;
      }
      if (e.key === "x" || e.key === "X") {
        const cb = document.getElementById("exclude-input");
        cb.checked = !cb.checked;
        toggleExclude(cb.checked);
        e.preventDefault();
        return;
      }
      if (e.key === "n" || e.key === "N") {
        document.getElementById("notes-textarea").focus();
        e.preventDefault();
      }
    });
  }

  function setupViewer() {
    const container = document.getElementById("viewer-3d");
    state.viewer = new Viewer3D(container);
    const ok = state.viewer.mount();
    if (ok) setStatus("Pick an embryo from the sidebar.");
  }

  // ----- catalog -----

  async function loadCatalog() {
    setStatus("Loading datasets…");
    try {
      const data = await API.datasets();
      state.datasets = data.datasets;
      state.stages = data.stages;
      await loadAnnotationSummary();  // before first render so badges show
      renderCatalog();
      setStatus("Pick an embryo from the sidebar.");
    } catch (err) {
      setStatus("Failed to load datasets:\n" + err.message, true);
    }
  }

  async function loadAnnotationSummary() {
    if (!state.annotator) {
      state.annotationSummary = new Map();
      return;
    }
    try {
      const data = await API.annotationSummary(state.annotator);
      state.annotationSummary = new Map(
        (data.items || []).map((it) => [
          summaryKey(it.dataset, it.session, it.embryo),
          { transitions: it.transitions, notes: it.notes, excluded: it.excluded },
        ])
      );
    } catch (err) {
      console.error("annotation summary failed:", err);
      state.annotationSummary = new Map();
    }
  }

  /** Reload summary + re-render sidebar after a save. */
  async function refreshAnnotationSummary() {
    await loadAnnotationSummary();
    renderCatalog();
  }

  function renderCatalog() {
    const root = document.getElementById("catalog");
    root.innerHTML = "";

    for (const ds of state.datasets) {
      const dsKey = ds.name;
      const dsHeader = document.createElement("div");
      dsHeader.className = "cat-dataset" + (ds.available ? "" : " unavailable");
      dsHeader.textContent = ds.name + (ds.available ? "" : " (offline)");
      dsHeader.title = ds.root || "no root configured";
      root.appendChild(dsHeader);

      if (!ds.available) {
        const empty = document.createElement("div");
        empty.className = "cat-empty";
        empty.textContent = "no root reachable";
        root.appendChild(empty);
        continue;
      }

      const isExpanded = state.expanded.datasets.has(dsKey);
      const dsRow = document.createElement("div");
      dsRow.className = "cat-row" + (isExpanded ? " expanded" : "");
      dsRow.innerHTML = `<span class="cat-caret">${isExpanded ? "▾" : "▸"}</span>` +
                       `<span class="cat-label">sessions</span>`;
      dsRow.addEventListener("click", () => toggleDataset(dsKey));
      root.appendChild(dsRow);

      if (isExpanded) {
        renderSessions(root, dsKey);
      }
    }
  }

  async function toggleDataset(dsKey) {
    if (state.expanded.datasets.has(dsKey)) {
      state.expanded.datasets.delete(dsKey);
      renderCatalog();
      return;
    }
    state.expanded.datasets.add(dsKey);
    await ensureSessionList(dsKey);
    renderCatalog();
  }

  async function ensureSessionList(dsKey) {
    if (state.sessionLists.has(dsKey)) return;
    if (state.sessionLoading.has(dsKey)) return;
    state.sessionLoading.add(dsKey);
    renderCatalog();
    try {
      const data = await API.sessions(dsKey, { includeEmpty: state.showEmpty });
      state.sessionLists.set(dsKey, data);
    } catch (err) {
      state.sessionLists.set(dsKey, { sessions: [], hidden_empty: 0, total_sessions: 0, shown: 0 });
      console.error("sessions load failed:", err);
    } finally {
      state.sessionLoading.delete(dsKey);
    }
  }

  async function toggleShowEmpty() {
    state.showEmpty = !state.showEmpty;
    state.sessionLists.clear();
    // Re-fetch any expanded datasets.
    const expandedKeys = Array.from(state.expanded.datasets);
    renderCatalog();
    await Promise.all(expandedKeys.map(ensureSessionList));
    renderCatalog();
  }

  function renderSessions(root, dsKey) {
    if (state.sessionLoading.has(dsKey)) {
      const loading = document.createElement("div");
      loading.className = "cat-empty";
      loading.textContent = "scanning sessions…";
      root.appendChild(loading);
      return;
    }
    const data = state.sessionLists.get(dsKey);
    if (!data) return;
    const sessions = data.sessions || [];

    if (data.hidden_empty > 0) {
      const note = document.createElement("div");
      note.className = "cat-hidden-note";
      note.innerHTML = `${sessions.length} of ${data.total_sessions} sessions · ` +
                       `<a href="#" class="show-empty-link">show ${data.hidden_empty} empty</a>`;
      note.querySelector(".show-empty-link").addEventListener("click", (e) => {
        e.preventDefault();
        toggleShowEmpty();
      });
      root.appendChild(note);
    } else if (state.showEmpty && data.total_sessions > 0) {
      const note = document.createElement("div");
      note.className = "cat-hidden-note";
      note.innerHTML = `${sessions.length} sessions (incl. empty) · ` +
                       `<a href="#" class="show-empty-link">hide empty</a>`;
      note.querySelector(".show-empty-link").addEventListener("click", (e) => {
        e.preventDefault();
        toggleShowEmpty();
      });
      root.appendChild(note);
    }

    if (sessions.length === 0) {
      const empty = document.createElement("div");
      empty.className = "cat-empty";
      empty.textContent = data.total_sessions === 0 ? "no sessions" : "all sessions are empty";
      root.appendChild(empty);
      return;
    }

    for (const s of sessions) {
      const key = `${dsKey}/${s.session_id}`;
      const isExpanded = state.expanded.sessions.has(key);
      const row = document.createElement("div");
      const isEmpty = s.embryo_count === 0;
      const annotated = countAnnotatedEmbryosInSession(dsKey, s.session_id);
      row.className = "cat-row session" + (isExpanded ? " expanded" : "") +
                      (isEmpty ? " empty" : "") +
                      (annotated > 0 ? " has-annotations" : "");
      const annotBadge = annotated > 0
        ? `<span class="cat-annot" title="${annotated} embryo${annotated === 1 ? "" : "s"} have annotations">${annotated} ●</span>`
        : "";
      row.innerHTML =
        `<span class="cat-caret">${isExpanded ? "▾" : "▸"}</span>` +
        `<span class="cat-label">${s.session_id}</span>` +
        `<span class="cat-meta">${s.embryo_count} emb · ${s.timepoint_count} tp</span>` +
        annotBadge;
      row.addEventListener("click", () => toggleSession(dsKey, s.session_id));
      root.appendChild(row);
      if (isExpanded) renderEmbryos(root, dsKey, s.session_id);
    }
  }

  function countAnnotatedEmbryosInSession(dataset, session) {
    let n = 0;
    for (const k of state.annotationSummary.keys()) {
      if (k.startsWith(`${dataset}/${session}/`)) n++;
    }
    return n;
  }

  async function toggleSession(dsKey, sid) {
    const key = `${dsKey}/${sid}`;
    if (state.expanded.sessions.has(key)) {
      state.expanded.sessions.delete(key);
    } else {
      state.expanded.sessions.add(key);
      if (!state.sessionEmbryos.has(key)) {
        try {
          const data = await API.embryos(dsKey, sid);
          state.sessionEmbryos.set(key, data.embryos);
        } catch (err) {
          state.sessionEmbryos.set(key, []);
          console.error("embryos load failed:", err);
        }
      }
    }
    renderCatalog();
  }

  function renderEmbryos(root, dsKey, sid) {
    const key = `${dsKey}/${sid}`;
    const embryos = state.sessionEmbryos.get(key) || [];
    if (embryos.length === 0) {
      const empty = document.createElement("div");
      empty.className = "cat-empty";
      empty.textContent = "no embryos";
      root.appendChild(empty);
      return;
    }
    const totalStages = state.stages.length || 7;
    for (const em of embryos) {
      const row = document.createElement("div");
      const isSelected =
        state.selected.dataset === dsKey &&
        state.selected.session === sid &&
        state.selected.embryo === em.embryo_id;
      const summary = state.annotationSummary.get(summaryKey(dsKey, sid, em.embryo_id));
      const hasAnnot = !!summary;
      row.className = "cat-row embryo" +
        (isSelected ? " selected" : "") +
        (hasAnnot ? " has-annotations" : "") +
        (summary?.excluded ? " excluded" : "");

      const badges = [];
      if (summary?.transitions) {
        badges.push(`<span class="cat-badge stages" title="stage transitions marked">${summary.transitions}/${totalStages}</span>`);
      }
      if (summary?.notes) {
        badges.push(`<span class="cat-badge notes" title="${summary.notes} note${summary.notes === 1 ? "" : "s"}">${summary.notes}✎</span>`);
      }
      if (summary?.excluded) {
        badges.push(`<span class="cat-badge bad" title="excluded">✕</span>`);
      }

      row.innerHTML =
        `<span class="cat-label">${em.embryo_id}</span>` +
        `<span class="cat-meta">${em.timepoint_count} tp</span>` +
        (badges.length ? `<span class="cat-badges">${badges.join("")}</span>` : "");
      row.addEventListener("click", () => selectEmbryo(dsKey, sid, em.embryo_id));
      root.appendChild(row);
    }
  }

  // ----- selection / volume load -----

  async function selectEmbryo(dataset, session, embryo) {
    flushNoteSave();  // commit any pending note for the previous embryo
    state.selected = { dataset, session, embryo, timepoint: null };
    state.annotations = {
      loaded: false,
      transitions: [],
      notes: new Map(),
      flag: { excluded: false, notes: null },
    };
    renderCatalog();
    updateBreadcrumb();
    renderAnnotationUI();  // disabled state until annotations load

    setStatus("Loading timepoints…");
    try {
      const data = await API.timepoints(dataset, session, embryo);
      state.timepoints = data.timepoints || [];
      if (state.timepoints.length === 0) {
        setStatus("No timepoints for this embryo.");
        return;
      }
      // Annotations and first volume in parallel — neither blocks the other.
      const annotPromise = loadAnnotations();
      await loadTimepoint(state.timepoints[0]);
      await annotPromise;
    } catch (err) {
      setStatus("Embryo load failed:\n" + err.message, true);
    }
  }

  async function loadAnnotations() {
    const { dataset, session, embryo } = state.selected;
    if (!dataset || !state.annotator) {
      renderAnnotationUI();
      return;
    }
    try {
      const data = await API.annotations(dataset, session, embryo, state.annotator);
      state.annotations.transitions = data.transitions || [];
      state.annotations.notes = new Map(
        (data.notes || []).map((n) => [n.timepoint, n.note])
      );
      state.annotations.flag = data.flag
        ? { excluded: !!data.flag.excluded, notes: data.flag.notes }
        : { excluded: false, notes: null };
      state.annotations.loaded = true;
    } catch (err) {
      console.error("loadAnnotations failed:", err);
    }
    renderAnnotationUI();
  }

  async function loadTimepoint(tp) {
    if (state.loadingVolume) return;
    flushNoteSave();  // commit any pending note for the previous timepoint
    state.loadingVolume = true;
    state.selected.timepoint = tp;
    updateTimepointReadout();
    renderAnnotationUI();  // re-render to update note, stage highlight, cursor
    setStatus(`Loading volume t=${tp}…\n(over network this can take 5–10 sec)`);

    const t0 = performance.now();
    try {
      const { dataset, session, embryo } = state.selected;
      const data = await API.volume(dataset, session, embryo, tp);
      const elapsed = ((performance.now() - t0) / 1000).toFixed(1);
      state.viewer.setVolume({
        data: data.data,
        shape: data.shape,
        voxelSizeUm: data.voxel_size_um,
      });
      setStatus(`t=${tp} · shape ${data.shape.join("×")} · ${elapsed}s`);
      setTimeout(() => clearStatus(), 1500);
    } catch (err) {
      setStatus(`Volume t=${tp} failed:\n${err.message}`, true);
    } finally {
      state.loadingVolume = false;
    }
  }

  // ---- annotations: stage transitions ----

  /** Stage at timepoint T = the latest transition with timepoint <= T. */
  function stageAtTimepoint(tp) {
    let best = null;
    for (const t of state.annotations.transitions) {
      if (t.timepoint <= tp && (!best || t.timepoint > best.timepoint)) best = t;
    }
    return best ? best.stage : null;
  }

  /** Toggle a stage marker at the current timepoint.
   *
   *   - If `stage` is already pinned at the current tp → DELETE the marker.
   *   - Else upsert it to the current tp (creates if missing, moves if pinned elsewhere).
   *
   * This makes "re-click to remove" the natural editing affordance instead of
   * relying on the Backspace hotkey.
   */
  async function markStage(stage) {
    if (!requireAnnotatorOrPrompt()) return;
    if (!state.selected.embryo) return;
    const tp = state.selected.timepoint;
    if (tp == null) return;

    const existing = state.annotations.transitions.find((t) => t.stage === stage);
    if (existing && existing.timepoint === tp) {
      await clearStageMarker(stage);
      return;
    }

    const { dataset, session, embryo } = state.selected;
    try {
      await API.upsertTransition(dataset, session, embryo, {
        annotator: state.annotator, stage, timepoint: tp,
      });
      if (existing) existing.timepoint = tp;
      else state.annotations.transitions.push({ stage, timepoint: tp });
      renderAnnotationUI();
      refreshAnnotationSummary();
    } catch (err) {
      setStatus(`Save failed: ${err.message}`, true);
    }
  }

  async function clearStageMarker(stage) {
    if (!requireAnnotatorOrPrompt()) return;
    const { dataset, session, embryo } = state.selected;
    try {
      await API.deleteTransition(dataset, session, embryo, stage, state.annotator);
      state.annotations.transitions = state.annotations.transitions.filter(
        (x) => x.stage !== stage
      );
      renderAnnotationUI();
      refreshAnnotationSummary();
    } catch (err) {
      setStatus(`Delete failed: ${err.message}`, true);
    }
  }

  /** Backspace hotkey — remove whichever marker is pinned at the current tp. */
  async function clearMarkerAtCurrentTp() {
    const tp = state.selected.timepoint;
    if (tp == null) return;
    const t = state.annotations.transitions.find((x) => x.timepoint === tp);
    if (!t) return;
    await clearStageMarker(t.stage);
  }

  // ---- annotations: notes ----

  function scheduleNoteSave() {
    if (state.noteSaveTimer) clearTimeout(state.noteSaveTimer);
    setNoteStatus("editing", "");
    state.noteSaveTimer = setTimeout(flushNoteSave, NOTE_DEBOUNCE_MS);
  }

  async function flushNoteSave() {
    if (state.noteSaveTimer) {
      clearTimeout(state.noteSaveTimer);
      state.noteSaveTimer = null;
    }
    const tp = state.selected.timepoint;
    if (tp == null || !state.selected.embryo) return;
    if (!state.annotator) return;
    const ta = document.getElementById("notes-textarea");
    if (!ta) return;
    const text = ta.value;
    const stored = state.annotations.notes.get(tp) || "";
    if (text === stored) {
      setNoteStatus("", "");
      return;
    }
    state.noteSaveInflight = true;
    setNoteStatus("saving", "saving…");
    try {
      const { dataset, session, embryo } = state.selected;
      await API.upsertNote(dataset, session, embryo, tp, {
        annotator: state.annotator, note: text,
      });
      if (text.trim()) state.annotations.notes.set(tp, text);
      else state.annotations.notes.delete(tp);
      setNoteStatus("saved", "saved");
      setTimeout(() => setNoteStatus("", ""), 1200);
      renderTimelineNotes();
      refreshAnnotationSummary();
    } catch (err) {
      setNoteStatus("error", "save failed");
      console.error("note save failed:", err);
    } finally {
      state.noteSaveInflight = false;
    }
  }

  function setNoteStatus(cls, text) {
    const el = document.getElementById("notes-status");
    if (!el) return;
    el.className = "notes-status" + (cls ? " " + cls : "");
    el.textContent = text;
  }

  // ---- annotations: exclude ----

  async function toggleExclude(excluded) {
    if (!requireAnnotatorOrPrompt()) return;
    const { dataset, session, embryo } = state.selected;
    if (!embryo) return;
    try {
      await API.upsertFlag(dataset, session, embryo, {
        annotator: state.annotator, excluded,
      });
      state.annotations.flag = { excluded, notes: state.annotations.flag.notes };
      renderExcludeUI();
      refreshAnnotationSummary();
    } catch (err) {
      setStatus(`Flag save failed: ${err.message}`, true);
    }
  }

  function requireAnnotatorOrPrompt() {
    if (state.annotator) return true;
    state.editingAnnotator = true;
    renderAnnotator();
    setStatus("Set your annotator name first.", true);
    setTimeout(clearStatus, 1800);
    return false;
  }

  // ---- rendering ----

  function renderAnnotationUI() {
    renderStageButtons();
    renderTimeline();
    renderNoteForCurrentTp();
    renderExcludeUI();
    updateAnnotTpLabel();
  }

  function updateAnnotTpLabel() {
    const el = document.getElementById("annot-tp-label");
    if (!el) return;
    const tp = state.selected.timepoint;
    el.textContent = tp != null ? `t=${tp}` : "—";
  }

  function renderStageButtons() {
    const root = document.getElementById("stage-buttons");
    if (!root) return;
    const tp = state.selected.timepoint;
    const interpolated = tp != null ? stageAtTimepoint(tp) : null;
    const markerByStage = new Map(
      state.annotations.transitions.map((t) => [t.stage, t.timepoint])
    );
    const enabled = state.annotations.loaded && tp != null;

    root.innerHTML = state.stages.map((stage, i) => {
      const markerTp = markerByStage.get(stage);
      const hasMarker = markerTp != null;
      const isMarkedHere = markerTp === tp;
      const isCurrent = stage === interpolated;

      const klass = ["stage-btn"];
      if (isMarkedHere) klass.push("marked-here");
      else if (isCurrent) klass.push("current");
      else if (hasMarker) klass.push("has-marker");
      if (!enabled) klass.push("disabled");

      const color = STAGE_COLORS[stage] || "#888";
      let title;
      if (!enabled) {
        title = `Pick a timepoint, then click to mark.`;
      } else if (isMarkedHere) {
        title = `"${stage}" starts here. Click again (or press Backspace) to remove.`;
      } else if (hasMarker) {
        title = `"${stage}" is marked at t=${markerTp}. Click to MOVE it to t=${tp}.`;
      } else {
        title = `Mark t=${tp} as the start of "${stage}".`;
      }

      // Use ✕ when marked-here so the action (remove) is obvious. Use ●
      // when marked at a different tp (just an indicator).
      const indicator = isMarkedHere ? " ✕" : (hasMarker ? " ●" : "");
      return `<button class="${klass.join(" ")}" data-stage="${escapeHtml(stage)}"
                       style="--stage-color: ${color};"
                       title="${escapeHtml(title)}">
                <span class="stage-key">${i + 1}</span>${escapeHtml(stage)}${indicator}
              </button>`;
    }).join("");

    root.querySelectorAll(".stage-btn").forEach((btn) => {
      btn.addEventListener("click", () => markStage(btn.dataset.stage));
    });
  }

  function renderTimeline() {
    const strip = document.getElementById("timeline-strip");
    const bands = document.getElementById("timeline-bands");
    const ticks = document.getElementById("timeline-ticks");
    const markers = document.getElementById("timeline-markers");
    const cursor = document.getElementById("timeline-cursor");
    if (!strip || !bands || !markers || !cursor) return;

    const enabled = state.timepoints.length > 0;
    strip.classList.toggle("disabled", !enabled);
    if (!enabled) {
      bands.innerHTML = ""; markers.innerHTML = "";
      if (ticks) ticks.innerHTML = "";
      cursor.style.display = "none";
      renderTimelineNotes();
      return;
    }

    const tps = state.timepoints;
    const N = tps.length;
    const xFor = (idx) => (N <= 1 ? 0 : (idx / (N - 1)) * 100);
    const widthFor = (idxStart, idxEnd) => xFor(idxEnd) - xFor(idxStart);

    // Build stage bands by walking timepoints and grouping by interpolated stage.
    bands.innerHTML = "";
    let runStart = 0;
    let runStage = stageAtTimepoint(tps[0]);
    for (let i = 1; i <= N; i++) {
      const cur = i < N ? stageAtTimepoint(tps[i]) : null;
      if (i === N || cur !== runStage) {
        if (runStage) {
          const band = document.createElement("div");
          band.className = "tl-band";
          band.style.left = xFor(runStart) + "%";
          band.style.width = widthFor(runStart, i - 1) + "%";
          band.style.background = STAGE_COLORS[runStage] || "#888";
          band.title = `${runStage}: t=${tps[runStart]} … t=${tps[i - 1]}`;
          bands.appendChild(band);
        }
        runStart = i;
        runStage = cur;
      }
    }

    // Transition markers — one triangle per (stage, timepoint).
    markers.innerHTML = "";
    for (const t of state.annotations.transitions) {
      const idx = tps.indexOf(t.timepoint);
      if (idx < 0) continue;
      const m = document.createElement("div");
      m.className = "tl-marker";
      m.style.left = xFor(idx) + "%";
      m.style.setProperty("--marker-color", STAGE_COLORS[t.stage] || "#fff");
      m.title = `${t.stage} starts at t=${t.timepoint}`;
      markers.appendChild(m);
    }

    renderTimelineTicks();
    renderTimelineNotes();

    // Current-timepoint cursor.
    const tp = state.selected.timepoint;
    if (tp == null) {
      cursor.style.display = "none";
    } else {
      const idx = tps.indexOf(tp);
      cursor.style.display = "block";
      cursor.style.left = xFor(idx) + "%";
    }
  }

  function renderTimelineTicks() {
    const ticks = document.getElementById("timeline-ticks");
    if (!ticks) return;
    ticks.innerHTML = "";
    const tps = state.timepoints;
    if (!tps.length) return;
    const xFor = (idx) => (tps.length <= 1 ? 0 : (idx / (tps.length - 1)) * 100);

    // Pick a tick spacing that yields ~6–10 major labels across the strip.
    const N = tps.length;
    const targetLabels = 8;
    const candidates = [1, 2, 5, 10, 20, 25, 50, 100, 200, 500, 1000];
    let major = candidates[candidates.length - 1];
    for (const c of candidates) {
      if (Math.ceil(N / c) <= targetLabels) { major = c; break; }
    }
    const minor = major >= 5 ? major / 5 : 0;

    for (let i = 0; i < N; i++) {
      const tp = tps[i];
      const isMajor = tp % major === 0 || i === 0 || i === N - 1;
      const isMinor = !isMajor && minor > 0 && tp % minor === 0;
      if (!isMajor && !isMinor) continue;
      const x = xFor(i);
      const tick = document.createElement("div");
      tick.className = "tl-tick" + (isMajor ? " major" : "");
      tick.style.left = x + "%";
      ticks.appendChild(tick);
      if (isMajor) {
        const label = document.createElement("div");
        label.className = "tl-tick-label";
        label.style.left = x + "%";
        label.textContent = String(tp);
        ticks.appendChild(label);
      }
    }
  }

  function renderTimelineNotes() {
    const notes = document.getElementById("timeline-notes");
    if (!notes) return;
    notes.innerHTML = "";
    const tps = state.timepoints;
    if (!tps.length) return;
    const xFor = (idx) => (tps.length <= 1 ? 0 : (idx / (tps.length - 1)) * 100);
    for (const tp of state.annotations.notes.keys()) {
      const idx = tps.indexOf(tp);
      if (idx < 0) continue;
      const dot = document.createElement("div");
      dot.className = "tl-note";
      dot.style.left = xFor(idx) + "%";
      dot.title = `note at t=${tp}`;
      notes.appendChild(dot);
    }
  }

  function renderNoteForCurrentTp() {
    const ta = document.getElementById("notes-textarea");
    if (!ta) return;
    const tp = state.selected.timepoint;
    const enabled = tp != null && state.annotations.loaded;
    ta.disabled = !enabled;
    if (!enabled) {
      ta.value = "";
      return;
    }
    const stored = state.annotations.notes.get(tp) || "";
    // Avoid clobbering the user's in-progress edit.
    if (document.activeElement !== ta) {
      ta.value = stored;
    }
  }

  function renderExcludeUI() {
    const cb = document.getElementById("exclude-input");
    const label = document.getElementById("exclude-label");
    if (!cb || !label) return;
    const enabled = state.annotations.loaded && !!state.selected.embryo;
    cb.disabled = !enabled;
    cb.checked = !!state.annotations.flag.excluded;
    label.classList.toggle("active", !!state.annotations.flag.excluded);
  }

  function stepTimepoint(delta) {
    const tps = state.timepoints;
    if (!tps.length || state.selected.timepoint == null) return;
    const i = tps.indexOf(state.selected.timepoint);
    if (i < 0) return;
    const j = i + delta;
    if (j < 0 || j >= tps.length) return;
    loadTimepoint(tps[j]);
  }

  // ----- header / status -----

  function updateBreadcrumb() {
    const el = document.getElementById("breadcrumb");
    const { dataset, session, embryo } = state.selected;
    if (!dataset) { el.textContent = ""; return; }
    el.innerHTML = `<b>${dataset}</b> / ${session || ""} / ${embryo || ""}`;
  }

  function updateTimepointReadout() {
    const el = document.getElementById("tp-readout");
    const tp = state.selected.timepoint;
    const tps = state.timepoints;
    if (!tps.length || tp == null) {
      el.textContent = "—";
      document.getElementById("prev-tp").disabled = true;
      document.getElementById("next-tp").disabled = true;
      return;
    }
    const i = tps.indexOf(tp);
    el.textContent = `t=${tp} (${i + 1}/${tps.length})`;
    document.getElementById("prev-tp").disabled = (i <= 0);
    document.getElementById("next-tp").disabled = (i >= tps.length - 1);

    const info = document.getElementById("timepoint-info");
    info.textContent = `${tps.length} timepoints`;
  }

  function setStatus(msg, isError = false) {
    const el = document.getElementById("viewer-status");
    el.textContent = msg;
    el.classList.toggle("error", isError);
    el.style.display = "block";
  }

  function clearStatus() {
    const el = document.getElementById("viewer-status");
    el.style.display = "none";
  }
})();
