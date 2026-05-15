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
    // Authoring vs viewing identity. state.annotator is who you ARE (your
    // saves get tagged with this). state.viewingAs is null when you're
    // editing your own work, or another annotator's name when you're
    // peeking at theirs read-only.
    viewingAs: null,
    knownAnnotators: [],          // populated from /api/annotations/annotators
    annotatorMenuOpen: false,
    addingAnnotator: false,
    loadingVolume: false,
    // Annotations for the currently selected embryo (current annotator only).
    annotations: {
      loaded: false,
      transitions: [],          // [{stage, timepoint, ...}]
      notes: new Map(),         // tp → note text
      flag: { excluded: false, notes: null },
      // tp → {ap_dir: [x,y,z]|null, dv_dir: [x,y,z]|null}
      orientations: new Map(),
      // [{id, start_tp, end_tp, notes}, ...]
      unreliableRanges: [],
      // [{id, timepoint, view_params, note, tag, ...}, ...]
      viewNotes: [],
      // Ground-truth overlay (HF datasets). Read-only — shown as a
      // distinct band in the timeline so the active annotator can compare
      // their own stage marks against the published labels.
      groundTruth: { transitions: [], annotator: null, source: null },
      // Twitching event from the per-embryo annotations.json. `mine` is
      // {timepoint, updated_at} for the active annotator; `others` is the
      // rest, surfaced for context but not editable.
      twitching: { mine: null, others: [] },
    },
    unreliableMarking: null,    // {startTp, startedAt} when user is mid-range
    // Summary across ALL of the current annotator's work — drives sidebar
    // badges. Keyed by "dataset/session/embryo" → {transitions, notes, excluded}.
    annotationSummary: new Map(),
    noteSaveTimer: null,
    noteSaveInflight: false,
    // Client-side LRU cache of fetched volumes — keyed by
    // "dataset/session/embryo/tp" → {shape, voxel_size_um, data: Uint8Array}.
    // Filled by both foreground loads and background prefetches; ~26 MB per
    // entry so cap is conservative.
    volumeCache: new Map(),
    volumeCacheMax: 6,
    prefetchInFlight: new Map(),  // key → AbortController
    // Pre-bake state for the currently selected embryo. Polled while the
    // server bakes sidecars in a process pool; once `running` flips to
    // false (and total>0), every fetch hits disk in ~80 ms.
    prebake: { dataset: null, session: null, embryo: null, total: 0, done: 0, errors: 0, alreadyComplete: 0, running: false },
    prebakePollTimer: null,
  };

  function volumeKey(ds, ss, em, tp) {
    return `${ds}/${ss}/${em}/${tp}`;
  }

  /** Whose annotations the UI should DISPLAY right now. When viewingAs is
   *  null (the default), that's you. When it's set, you're peeking at
   *  someone else's work in read-only mode. */
  function effectiveAnnotator() {
    return state.viewingAs || state.annotator;
  }

  /** True when the UI is in read-only "view someone else" mode. Every
   *  write action checks this and bails out early. */
  function isViewOnly() {
    return !!state.viewingAs;
  }

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
    await setupAnnotator();
    setupControls();
    setupViewer();
    await loadCatalog();
  });

  async function setupAnnotator() {
    const saved = (localStorage.getItem(ANNOTATOR_KEY) || "").trim();
    state.annotator = saved || null;
    state.viewingAs = null;
    state.addingAnnotator = !state.annotator;  // first-visit -> show add-new immediately
    await refreshKnownAnnotators();
    renderAnnotator();
  }

  async function refreshKnownAnnotators() {
    try {
      const data = await API.knownAnnotators();
      state.knownAnnotators = data.annotators || [];
    } catch (err) {
      console.warn("knownAnnotators load failed:", err.message);
      state.knownAnnotators = [];
    }
  }

  /** Single picker for both "switch identity" (edit-self) and "view as
   *  someone else" (read-only). The chip is the dropdown trigger; the
   *  menu lists you (with a ✎ icon) plus all known annotators (with 👁),
   *  ending with a "+ sign in as new..." option that becomes an inline
   *  input for renaming yourself or creating a new identity. */
  function renderAnnotator() {
    const root = document.getElementById("annotator-section");
    const flash = state.justSavedAnnotator ? " saved" : "";

    if (state.addingAnnotator) {
      root.innerHTML = `
        <label for="annotator-input">You:</label>
        <input id="annotator-input" type="text" placeholder="your name" autocomplete="off" />
        <button id="annotator-save" type="button">Save</button>
        ${state.annotator ? '<button id="annotator-cancel" type="button" class="link-btn">cancel</button>' : ''}
      `;
      const input = root.querySelector("#annotator-input");
      const saveBtn = root.querySelector("#annotator-save");
      input.value = state.annotator || "";
      setTimeout(() => input.focus(), 0);

      const commit = () => {
        const name = input.value.trim();
        if (!name) { input.focus(); return; }
        const changed = name !== state.annotator;
        state.annotator = name;
        state.viewingAs = null;
        state.addingAnnotator = false;
        state.annotatorMenuOpen = false;
        state.justSavedAnnotator = true;
        localStorage.setItem(ANNOTATOR_KEY, name);
        renderAnnotator();
        refreshKnownAnnotators();  // your name is now in the list
        if (changed) {
          flushNoteSave();
          if (state.selected.embryo) loadAnnotations();
          refreshAnnotationSummary();
          updateViewOnlyChrome();
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
          state.addingAnnotator = false;
          renderAnnotator();
        }
      });
      input.addEventListener("blur", () => {
        const v = input.value.trim();
        if (v && v !== state.annotator) commit();
      });
      const cancel = root.querySelector("#annotator-cancel");
      cancel?.addEventListener("click", () => {
        state.addingAnnotator = false;
        renderAnnotator();
      });
      return;
    }

    // Build the closed/open dropdown chip.
    const others = state.knownAnnotators.filter((n) => n !== state.annotator);
    const viewing = state.viewingAs;
    const chipName = state.annotator || "(no identity)";
    let html = `
      <span class="annotator-label">Signed in as</span>
      <button id="annotator-trigger" class="annotator-chip${flash} dropdown" type="button"
              aria-haspopup="listbox" aria-expanded="${state.annotatorMenuOpen ? "true" : "false"}">
        ${escapeHtml(chipName)} <span class="annotator-caret">▾</span>
      </button>
    `;
    if (state.annotatorMenuOpen) {
      const items = [];
      items.push(`
        <li class="ann-item ${!viewing ? "selected" : ""}" data-action="edit-self">
          <span class="ann-item-icon">✎</span>${escapeHtml(state.annotator || "")}
          <span class="ann-item-tag">(you)</span>
        </li>
      `);
      if (others.length) items.push(`<li class="ann-divider"></li>`);
      for (const name of others) {
        items.push(`
          <li class="ann-item ${viewing === name ? "selected" : ""}"
              data-action="view-as" data-name="${escapeHtml(name)}">
            <span class="ann-item-icon">👁</span>${escapeHtml(name)}
            <span class="ann-item-tag">view-only</span>
          </li>
        `);
      }
      items.push(`<li class="ann-divider"></li>`);
      items.push(`
        <li class="ann-item add" data-action="add-new">
          <span class="ann-item-icon">+</span>sign in as new…
        </li>
      `);
      html += `<ul id="annotator-menu" role="listbox">${items.join("")}</ul>`;
    }
    if (viewing) {
      html += `
        <div class="viewing-pill" id="viewing-pill" title="Viewing ${escapeHtml(viewing)}'s annotations">
          <span class="viewing-icon">👁</span>
          Viewing <b>${escapeHtml(viewing)}</b>
          <button id="viewing-stop" class="viewing-stop" type="button" aria-label="Stop viewing">✕</button>
        </div>
      `;
    }
    root.innerHTML = html;

    root.querySelector("#annotator-trigger")?.addEventListener("click", (e) => {
      e.stopPropagation();
      state.annotatorMenuOpen = !state.annotatorMenuOpen;
      renderAnnotator();
    });
    root.querySelector("#annotator-menu")?.addEventListener("click", (e) => {
      const li = e.target.closest("li.ann-item");
      if (!li) return;
      const action = li.dataset.action;
      if (action === "edit-self") {
        state.viewingAs = null;
        state.annotatorMenuOpen = false;
      } else if (action === "view-as") {
        state.viewingAs = li.dataset.name;
        state.annotatorMenuOpen = false;
      } else if (action === "add-new") {
        state.addingAnnotator = true;
        state.annotatorMenuOpen = false;
      }
      renderAnnotator();
      updateViewOnlyChrome();
      flushNoteSave();
      if (state.selected.embryo) loadAnnotations();
      refreshAnnotationSummary();
    });
    root.querySelector("#viewing-stop")?.addEventListener("click", () => {
      state.viewingAs = null;
      renderAnnotator();
      updateViewOnlyChrome();
      if (state.selected.embryo) loadAnnotations();
      refreshAnnotationSummary();
    });
  }

  // Click outside the dropdown / Esc -> close.
  document.addEventListener("click", () => {
    if (state.annotatorMenuOpen) {
      state.annotatorMenuOpen = false;
      renderAnnotator();
    }
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && state.annotatorMenuOpen) {
      state.annotatorMenuOpen = false;
      renderAnnotator();
    }
  });

  /** Toggle a class on the main column so a soft purple stripe + dim
   *  appearance signals view-only mode at-a-glance, regardless of
   *  whether the annotator chip is on screen. */
  function updateViewOnlyChrome() {
    document.body.classList.toggle("view-only", isViewOnly());
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

    // Snap-to-axis buttons.
    document.querySelectorAll(".snap-btn[data-snap]").forEach((btn) => {
      btn.addEventListener("click", () => {
        if (state.viewer) state.viewer.snapView(btn.dataset.snap);
      });
    });
    document.getElementById("snap-reset")?.addEventListener("click", () => {
      if (state.viewer) state.viewer.resetView();
    });

    // Notes textarea — auto-save on idle and on blur.
    const ta = document.getElementById("notes-textarea");
    ta.addEventListener("input", () => scheduleNoteSave());
    ta.addEventListener("blur", () => flushNoteSave());

    // Exclude checkbox.
    document.getElementById("exclude-input").addEventListener("change", (e) => {
      toggleExclude(e.target.checked);
    });

    // Orientation row.
    document.getElementById("view-note-add").addEventListener("click", () => captureViewNote());

    document.getElementById("orient-save-ap").addEventListener("click", () => saveAxisFromView("ap"));
    document.getElementById("orient-save-dv").addEventListener("click", () => saveAxisFromView("dv"));
    document.getElementById("orient-clear").addEventListener("click", () => clearOrientationHere());
    document.getElementById("orient-align").addEventListener("click", () => alignToSavedOrientation());
    document.getElementById("orient-unreliable").addEventListener("click", () => toggleUnreliableMark());
    document.getElementById("orient-show-axes").addEventListener("change", (e) => {
      if (state.viewer) state.viewer.setAxesVisible(e.target.checked);
    });

    // Events row (HF datasets only).
    document.getElementById("twitch-toggle")?.addEventListener("click", () => toggleTwitching());
    document.getElementById("twitch-clear")?.addEventListener("click", () => clearTwitching());

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
        return;
      }
      if (e.key === "a" || e.key === "A") {
        saveAxisFromView("ap"); e.preventDefault(); return;
      }
      if (e.key === "d" || e.key === "D") {
        saveAxisFromView("dv"); e.preventDefault(); return;
      }
      if (e.key === "u" || e.key === "U") {
        toggleUnreliableMark(); e.preventDefault(); return;
      }
      if (e.key === "o" || e.key === "O") {
        clearOrientationHere(); e.preventDefault(); return;
      }
      if (e.key === "r" || e.key === "R") {
        alignToSavedOrientation(); e.preventDefault(); return;
      }
      if (e.key === "t" || e.key === "T") {
        toggleTwitching(); e.preventDefault(); return;
      }
      if (e.key === "Escape" && state.unreliableMarking) {
        state.unreliableMarking = null;
        renderAnnotationUI();
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
    if (!effectiveAnnotator()) {
      state.annotationSummary = new Map();
      return;
    }
    try {
      const data = await API.annotationSummary(effectiveAnnotator());
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
      // Pre-bake progress badge — only on the currently selected embryo.
      if (
        isSelected &&
        state.prebake.embryo === em.embryo_id &&
        state.prebake.session === sid &&
        state.prebake.dataset === dsKey &&
        state.prebake.total > 0
      ) {
        const baked = state.prebake.alreadyComplete + state.prebake.done;
        const totalAll = state.prebake.alreadyComplete + state.prebake.total;
        const pct = totalAll > 0 ? Math.round((baked / totalAll) * 100) : 0;
        const cls = state.prebake.running ? "bake running" : "bake done";
        const label = state.prebake.running
          ? `⏳ ${baked}/${totalAll}`
          : `✓ baked`;
        const title = state.prebake.running
          ? `Pre-baking previews: ${baked} of ${totalAll} (${pct}%)`
          : `All ${totalAll} previews baked.`;
        badges.push(`<span class="cat-badge ${cls}" title="${title}">${label}</span>`);
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
    cancelPrefetches();
    stopPrebakePolling();
    state.volumeCache.clear();   // different embryo → different volumes
    state.selected = { dataset, session, embryo, timepoint: null };
    state.annotations = {
      loaded: false,
      transitions: [],
      notes: new Map(),
      flag: { excluded: false, notes: null },
      orientations: new Map(),
      unreliableRanges: [],
      viewNotes: [],
      groundTruth: { transitions: [], annotator: null, source: null },
      twitching: { mine: null, others: [] },
    };
    state.unreliableMarking = null;
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
      const prebakePromise = startPrebake(dataset, session, embryo);
      await loadTimepoint(state.timepoints[0]);
      await annotPromise;
      await prebakePromise;
    } catch (err) {
      setStatus("Embryo load failed:\n" + err.message, true);
    }
  }

  // ---- pre-bake ----

  async function startPrebake(dataset, session, embryo) {
    try {
      const status = await API.prebakeStart(dataset, session, embryo);
      applyPrebakeStatus(dataset, session, embryo, status);
      if (status.total > 0 && status.running) {
        startPrebakePolling();
      }
    } catch (err) {
      console.warn("prebake start failed:", err.message);
    }
  }

  function startPrebakePolling() {
    stopPrebakePolling();
    state.prebakePollTimer = setInterval(async () => {
      const { dataset, session, embryo } = state.selected;
      if (!dataset || !embryo) { stopPrebakePolling(); return; }
      try {
        const status = await API.prebakeStatus(dataset, session, embryo);
        applyPrebakeStatus(dataset, session, embryo, status);
        if (!status.running) stopPrebakePolling();
      } catch (err) {
        console.warn("prebake status poll failed:", err.message);
      }
    }, 1500);
  }

  function stopPrebakePolling() {
    if (state.prebakePollTimer) {
      clearInterval(state.prebakePollTimer);
      state.prebakePollTimer = null;
    }
  }

  function applyPrebakeStatus(dataset, session, embryo, s) {
    state.prebake = {
      dataset, session, embryo,
      total: s.total || 0,
      done: s.done || 0,
      errors: s.errors || 0,
      alreadyComplete: s.already_complete || 0,
      running: !!s.running,
    };
    renderCatalog();   // sidebar progress badge
  }

  async function loadAnnotations() {
    const { dataset, session, embryo } = state.selected;
    if (!dataset || !state.annotator) {
      renderAnnotationUI();
      return;
    }
    try {
      const data = await API.annotations(dataset, session, embryo, effectiveAnnotator());
      state.annotations.transitions = data.transitions || [];
      state.annotations.notes = new Map(
        (data.notes || []).map((n) => [n.timepoint, n.note])
      );
      state.annotations.flag = data.flag
        ? { excluded: !!data.flag.excluded, notes: data.flag.notes }
        : { excluded: false, notes: null };
      state.annotations.orientations = new Map(
        (data.orientations || []).map((o) => [
          o.timepoint,
          { ap_dir: o.ap_dir || null, dv_dir: o.dv_dir || null },
        ])
      );
      state.annotations.unreliableRanges = data.unreliable_ranges || [];
      state.annotations.viewNotes = data.view_notes || [];
      const gt = data.ground_truth || {};
      state.annotations.groundTruth = {
        transitions: gt.transitions || [],
        annotator: gt.annotator || null,
        source: gt.source || null,
      };
      const ev = (data.events && data.events.twitching_start) || {};
      state.annotations.twitching = {
        mine: ev.mine || null,
        others: ev.others || [],
      };
      state.annotations.loaded = true;
    } catch (err) {
      console.error("loadAnnotations failed:", err);
    }
    renderAnnotationUI();
  }

  /** Fetch a volume, returning from the client cache if possible. Foreground
   *  call. Updates LRU on hit. */
  async function fetchVolumeCached(ds, ss, em, tp) {
    const key = volumeKey(ds, ss, em, tp);
    const cached = state.volumeCache.get(key);
    if (cached) {
      // LRU: move to most-recently-used.
      state.volumeCache.delete(key);
      state.volumeCache.set(key, cached);
      return { ...cached, fromCache: true };
    }
    // If a prefetch is already in flight for this key, await its result by
    // polling the cache — simpler than wiring up a shared promise.
    if (state.prefetchInFlight.has(key)) {
      for (let i = 0; i < 200; i++) {  // up to ~10s
        await new Promise((r) => setTimeout(r, 50));
        const c = state.volumeCache.get(key);
        if (c) {
          state.volumeCache.delete(key);
          state.volumeCache.set(key, c);
          return { ...c, fromCache: true };
        }
        if (!state.prefetchInFlight.has(key)) break; // failed
      }
    }
    const data = await API.volume(ds, ss, em, tp);
    putInVolumeCache(key, data);
    return { ...data, fromCache: false };
  }

  function putInVolumeCache(key, data) {
    state.volumeCache.set(key, data);
    while (state.volumeCache.size > state.volumeCacheMax) {
      const oldest = state.volumeCache.keys().next().value;
      state.volumeCache.delete(oldest);
    }
  }

  /** Background prefetch of neighbor timepoints. Doesn't await; failures
   *  are silently logged. */
  function schedulePrefetch(ds, ss, em, tps, currentTp) {
    if (!tps.length) return;
    const idx = tps.indexOf(currentTp);
    if (idx < 0) return;
    // Order matters: nearest neighbors first (most likely next press).
    const deltas = [1, -1, 2, -2];
    for (const delta of deltas) {
      const j = idx + delta;
      if (j < 0 || j >= tps.length) continue;
      const tp = tps[j];
      const key = volumeKey(ds, ss, em, tp);
      if (state.volumeCache.has(key)) continue;
      if (state.prefetchInFlight.has(key)) continue;
      const ctrl = new AbortController();
      state.prefetchInFlight.set(key, ctrl);
      API.volume(ds, ss, em, tp, { signal: ctrl.signal })
        .then((data) => putInVolumeCache(key, data))
        .catch((err) => {
          if (err.name !== "AbortError") {
            console.warn("prefetch failed for t=" + tp + ":", err.message);
          }
        })
        .finally(() => {
          state.prefetchInFlight.delete(key);
        });
    }
  }

  /** Cancel any in-flight prefetches — called when the user changes embryos. */
  function cancelPrefetches() {
    for (const ctrl of state.prefetchInFlight.values()) {
      try { ctrl.abort(); } catch (_) {}
    }
    state.prefetchInFlight.clear();
  }

  async function loadTimepoint(tp) {
    if (state.loadingVolume) return;
    flushNoteSave();  // commit any pending note for the previous timepoint
    state.loadingVolume = true;
    state.selected.timepoint = tp;
    updateTimepointReadout();
    renderAnnotationUI();
    const { dataset, session, embryo } = state.selected;
    const cacheHit = state.volumeCache.has(volumeKey(dataset, session, embryo, tp));
    if (!cacheHit) {
      setStatus(`Loading volume t=${tp}…`);
    }

    const t0 = performance.now();
    try {
      const data = await fetchVolumeCached(dataset, session, embryo, tp);
      const elapsed = ((performance.now() - t0) / 1000).toFixed(2);
      state.viewer.setVolume({
        data: data.data,
        shape: data.shape,
        voxelSizeUm: data.voxel_size_um,
      });
      // If a view-note "restore" queued specific view params before the
      // navigation, apply them now that the new volume is rendered.
      if (state._pendingViewParams) {
        const p = state._pendingViewParams;
        state._pendingViewParams = null;
        applyPendingViewParams(p);
      }
      const tag = data.fromCache ? "cached" : `${elapsed}s`;
      setStatus(`t=${tp} · ${data.shape.join("×")} · ${tag}`);
      setTimeout(() => clearStatus(), 900);
    } catch (err) {
      setStatus(`Volume t=${tp} failed:\n${err.message}`, true);
    } finally {
      state.loadingVolume = false;
    }
    // Warm the neighbors. Idle CPU + bandwidth — non-blocking.
    schedulePrefetch(dataset, session, embryo, state.timepoints, tp);
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
    if (isViewOnly()) return readOnlyToast();
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
    if (isViewOnly()) return readOnlyToast();
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

  // ---- annotations: twitching event (HF datasets only) ----

  /** True when the loaded embryo has a per-embryo annotations.json
   *  sidecar — i.e., it's the HuggingFace benchmark dataset. */
  function hasSidecar() {
    return state.annotations.groundTruth.source === "annotations.json";
  }

  /** Toggle the active annotator's twitching_start mark at the current
   *  timepoint. Re-clicking on the same tp clears the mark. */
  async function toggleTwitching() {
    if (isViewOnly()) return readOnlyToast();
    if (!requireAnnotatorOrPrompt()) return;
    if (!state.selected.embryo) return;
    if (!hasSidecar()) {
      setStatus("Twitching events are only supported on the HuggingFace dataset.", true);
      return;
    }
    const tp = state.selected.timepoint;
    if (tp == null) return;
    const { dataset, session, embryo } = state.selected;
    const mine = state.annotations.twitching.mine;
    try {
      if (mine && mine.timepoint === tp) {
        await API.deleteTwitching(dataset, session, embryo, state.annotator);
        state.annotations.twitching.mine = null;
      } else {
        await API.upsertTwitching(dataset, session, embryo, {
          annotator: state.annotator, timepoint: tp,
        });
        state.annotations.twitching.mine = { annotator: state.annotator, timepoint: tp };
      }
      renderAnnotationUI();
    } catch (err) {
      setStatus(`Twitching save failed: ${err.message}`, true);
    }
  }

  async function clearTwitching() {
    if (isViewOnly()) return readOnlyToast();
    if (!requireAnnotatorOrPrompt()) return;
    if (!state.selected.embryo) return;
    if (!hasSidecar()) return;
    const { dataset, session, embryo } = state.selected;
    try {
      await API.deleteTwitching(dataset, session, embryo, state.annotator);
      state.annotations.twitching.mine = null;
      renderAnnotationUI();
    } catch (err) {
      setStatus(`Twitching clear failed: ${err.message}`, true);
    }
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
    if (isViewOnly()) return;  // textarea is disabled, but be defensive
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

  // ---- annotations: orientation ----

  async function saveAxisFromView(axis) {
    if (isViewOnly()) return readOnlyToast();
    if (!requireAnnotatorOrPrompt()) return;
    const tp = state.selected.timepoint;
    if (tp == null || !state.viewer) return;
    // Conventions:
    //   AP (anterior-posterior) — anterior is the LEFT of the screen.
    //     The user lays the embryo out horizontally, head on the left.
    //   DV (dorsal-ventral)     — dorsal is the TOP of the screen.
    const dir = axis === "ap"
      ? state.viewer.captureLocalLeft()
      : state.viewer.captureLocalUp();
    if (!dir) {
      setStatus("Could not capture orientation (viewer not ready).", true);
      return;
    }
    const { dataset, session, embryo } = state.selected;
    try {
      await API.upsertOrientationAxis(dataset, session, embryo, tp, {
        annotator: state.annotator, axis, direction: dir,
      });
      const row = state.annotations.orientations.get(tp) || { ap_dir: null, dv_dir: null };
      if (axis === "ap") row.ap_dir = dir; else row.dv_dir = dir;
      state.annotations.orientations.set(tp, row);
      renderAnnotationUI();
      refreshAnnotationSummary();
    } catch (err) {
      setStatus(`Save failed: ${err.message}`, true);
    }
  }

  /** Restore the view to the saved AP/DV orientation at the current tp.
   *  If only one axis is saved, do the shortest rotation to put that one
   *  on its canonical position. If neither is saved, a brief status
   *  message tells the user there's nothing to align to. */
  function alignToSavedOrientation() {
    if (!state.viewer) return;
    const tp = state.selected.timepoint;
    if (tp == null) return;
    const o = state.annotations.orientations.get(tp);
    if (!o || (!o.ap_dir && !o.dv_dir)) {
      setStatus("No AP or DV saved at this timepoint to align to.", true);
      setTimeout(clearStatus, 1500);
      return;
    }
    const ok = state.viewer.alignToSavedOrientation(o.ap_dir, o.dv_dir);
    if (!ok) {
      setStatus("Could not align (viewer not ready).", true);
    }
  }

  async function clearOrientationHere() {
    if (isViewOnly()) return readOnlyToast();
    if (!requireAnnotatorOrPrompt()) return;
    const tp = state.selected.timepoint;
    if (tp == null) return;
    const { dataset, session, embryo } = state.selected;
    try {
      await API.clearOrientation(dataset, session, embryo, tp, state.annotator);
      state.annotations.orientations.delete(tp);
      renderAnnotationUI();
      refreshAnnotationSummary();
    } catch (err) {
      setStatus(`Clear failed: ${err.message}`, true);
    }
  }

  /** The current state of the unreliable-button does triple duty:
   *   - if the current timepoint is INSIDE an existing range, the button
   *     becomes "Remove range" and clicking deletes it.
   *   - if the user is mid-marking (one endpoint pinned), the button
   *     completes the range using the current timepoint.
   *   - otherwise, the button starts a new range with this tp as the start.
   */
  async function toggleUnreliableMark() {
    if (isViewOnly()) return readOnlyToast();
    if (!requireAnnotatorOrPrompt()) return;
    const tp = state.selected.timepoint;
    if (tp == null) return;

    // Mode 1: clicking while inside an existing range removes it.
    const containing = (state.annotations.unreliableRanges || []).find(
      (r) => tp >= r.start_tp && tp <= r.end_tp
    );
    if (containing && !state.unreliableMarking) {
      await deleteUnreliableRange(containing.id);
      return;
    }

    if (!state.unreliableMarking) {
      // Mode 3: pin a start.
      state.unreliableMarking = { startTp: tp };
      renderAnnotationUI();
      return;
    }

    // Mode 2: commit a range from the pinned start to current tp.
    const { dataset, session, embryo } = state.selected;
    const startTp = state.unreliableMarking.startTp;
    state.unreliableMarking = null;
    try {
      const r = await API.addUnreliableRange(dataset, session, embryo, {
        annotator: state.annotator,
        start_tp: Math.min(startTp, tp),
        end_tp: Math.max(startTp, tp),
      });
      state.annotations.unreliableRanges.push({
        id: r.id,
        start_tp: Math.min(startTp, tp),
        end_tp: Math.max(startTp, tp),
        notes: null,
      });
      renderAnnotationUI();
      refreshAnnotationSummary();
    } catch (err) {
      setStatus(`Save failed: ${err.message}`, true);
    }
  }

  async function deleteUnreliableRange(rangeId) {
    if (isViewOnly()) return readOnlyToast();
    if (!requireAnnotatorOrPrompt()) return;
    const { dataset, session, embryo } = state.selected;
    try {
      await API.deleteUnreliableRange(dataset, session, embryo, rangeId, state.annotator);
      state.annotations.unreliableRanges = state.annotations.unreliableRanges.filter(
        (r) => r.id !== rangeId
      );
      renderAnnotationUI();
      refreshAnnotationSummary();
    } catch (err) {
      setStatus(`Delete failed: ${err.message}`, true);
    }
  }

  // ---- annotations: exclude ----

  async function toggleExclude(excluded) {
    if (isViewOnly()) return readOnlyToast();
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

  function readOnlyToast() {
    setStatus(`Read-only — viewing ${state.viewingAs}'s annotations.`, true);
    setTimeout(clearStatus, 1400);
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
    renderOrientationUI();
    renderEventsUI();
    renderViewNotes();
    updateAnnotTpLabel();
  }

  function renderEventsUI() {
    const row = document.getElementById("events-row");
    if (!row) return;
    // The events row is HF-only — hide on Gently/Gently2.
    const show = hasSidecar();
    row.hidden = !show;
    if (!show) return;

    const btn = document.getElementById("twitch-toggle");
    const label = document.getElementById("twitch-toggle-label");
    const clearBtn = document.getElementById("twitch-clear");
    const stateEl = document.getElementById("twitch-state");
    const tp = state.selected.timepoint;
    const enabled = state.annotations.loaded && tp != null && !isViewOnly();
    btn.disabled = !enabled;
    const mine = state.annotations.twitching.mine;
    const isAtCurrent = mine && mine.timepoint === tp;
    btn.classList.toggle("has-ap", !!isAtCurrent);  // reuse green styling
    label.textContent = isAtCurrent
      ? `Twitching starts here (t=${tp}) — click again to clear`
      : (mine ? `Move twitching start to t=${tp}` : "Mark twitching start");

    clearBtn.hidden = !mine;
    clearBtn.disabled = !mine || isViewOnly();

    const parts = [];
    if (mine) parts.push(`<span class="ap">you: twitching @ t=${mine.timepoint}</span>`);
    for (const o of state.annotations.twitching.others || []) {
      parts.push(`<span class="dv">${escapeHtml(o.annotator)}: t=${o.timepoint}</span>`);
    }
    stateEl.innerHTML = parts.join(" · ");
  }

  function renderOrientationUI() {
    const tp = state.selected.timepoint;
    const enabled = state.annotations.loaded && tp != null && !isViewOnly();
    const apBtn = document.getElementById("orient-save-ap");
    const dvBtn = document.getElementById("orient-save-dv");
    const clrBtn = document.getElementById("orient-clear");
    const unrelBtn = document.getElementById("orient-unreliable");
    const unrelLabel = document.getElementById("orient-unreliable-label");
    const stateEl = document.getElementById("orient-state");
    if (!apBtn) return;

    [apBtn, dvBtn, clrBtn, unrelBtn].forEach((b) => (b.disabled = !enabled));

    const o = state.annotations.orientations.get(tp);
    apBtn.classList.toggle("has-ap", !!(o && o.ap_dir));
    dvBtn.classList.toggle("has-dv", !!(o && o.dv_dir));

    const inUnreliable = (state.annotations.unreliableRanges || []).find(
      (r) => tp >= r.start_tp && tp <= r.end_tp
    );

    unrelBtn.classList.remove("recording", "removing");
    if (state.unreliableMarking) {
      unrelBtn.classList.add("recording");
      unrelLabel.textContent = `Set range end (start t=${state.unreliableMarking.startTp})`;
      unrelBtn.title = "Click again to commit the unreliable range, or press Esc to cancel.";
    } else if (inUnreliable) {
      unrelBtn.classList.add("removing");
      unrelLabel.textContent = `Remove range t=${inUnreliable.start_tp}–${inUnreliable.end_tp}`;
      unrelBtn.title = `Delete this unreliable range.`;
    } else {
      unrelLabel.textContent = "Mark unreliable…";
      unrelBtn.title = "Mark a range of timepoints as unreliable (u to start, u again at end)";
    }
    const parts = [];
    if (o && o.ap_dir) parts.push('<span class="ap">AP set</span>');
    if (o && o.dv_dir) parts.push('<span class="dv">DV set</span>');
    if (inUnreliable) parts.push(`<span class="unrel">unreliable (t=${inUnreliable.start_tp}–${inUnreliable.end_tp})</span>`);
    stateEl.innerHTML = parts.join(" · ");
    stateEl.classList.toggle("unreliable", !!inUnreliable);

    // Push the gizmo to the viewer for the current tp's saved axes.
    if (state.viewer) {
      state.viewer.setOrientationAxes({
        ap: o?.ap_dir || null,
        dv: o?.dv_dir || null,
      });
    }
  }

  // ---- view notes ----

  /** Capture the current viewer state and prompt for a note + optional tag.
   *  Empty cancels. Saves to the server and re-renders. */
  async function captureViewNote() {
    if (isViewOnly()) return readOnlyToast();
    if (!requireAnnotatorOrPrompt()) return;
    const tp = state.selected.timepoint;
    if (tp == null || !state.viewer) {
      setStatus("Pick an embryo + timepoint first.", true);
      setTimeout(clearStatus, 1500);
      return;
    }
    const view_params = state.viewer.captureViewParams();
    const text = window.prompt(
      `View note for t=${tp}\n\n` +
      `Captures the current camera rotation, zoom, threshold, and contrast.\n` +
      `You can prefix with a tag — "best: …", "worst: …", "diagnostic: …" — and ` +
      `it'll be parsed as the tag.\n\nNote:`
    );
    if (text == null) return;
    const trimmed = text.trim();
    if (!trimmed) return;
    let tag = null;
    let note = trimmed;
    const m = trimmed.match(/^([A-Za-z][A-Za-z0-9_-]{0,23})\s*:\s*(.+)$/s);
    if (m) {
      tag = m[1].toLowerCase();
      note = m[2].trim();
    }

    const { dataset, session, embryo } = state.selected;
    try {
      const r = await API.addViewNote(dataset, session, embryo, {
        annotator: state.annotator, timepoint: tp, view_params, note, tag,
      });
      state.annotations.viewNotes.push({
        id: r.id, dataset, session, embryo, timepoint: tp,
        view_params, note, tag, annotator: state.annotator,
      });
      renderViewNotes();
      refreshAnnotationSummary();
    } catch (err) {
      setStatus(`Save failed: ${err.message}`, true);
    }
  }

  async function restoreViewNote(noteId) {
    const vn = state.annotations.viewNotes.find((x) => x.id === noteId);
    if (!vn || !state.viewer) return;
    if (vn.timepoint !== state.selected.timepoint) {
      // Jump to that tp first; viewer.applyViewParams runs after the volume loads.
      state._pendingViewParams = vn.view_params;
      await loadTimepoint(vn.timepoint);
    } else {
      applyPendingViewParams(vn.view_params);
    }
    // Reflect threshold/contrast in the slider DOM too.
    syncSlidersFromViewer();
  }

  /** Called from loadTimepoint() once the new volume is rendered, if a
   *  pending view-params snapshot is queued. */
  function applyPendingViewParams(p) {
    if (!state.viewer) return;
    state.viewer.applyViewParams(p);
    syncSlidersFromViewer();
  }

  function syncSlidersFromViewer() {
    if (!state.viewer) return;
    const t = document.getElementById("threshold");
    const c = document.getElementById("contrast");
    const tv = document.getElementById("threshold-val");
    const cv = document.getElementById("contrast-val");
    if (t) t.value = state.viewer.threshold;
    if (c) c.value = Math.round(state.viewer.contrast * 100);
    if (tv) tv.textContent = (state.viewer.threshold / 100).toFixed(2);
    if (cv) cv.textContent = state.viewer.contrast.toFixed(1);
  }

  async function deleteViewNote(noteId) {
    if (isViewOnly()) return readOnlyToast();
    if (!confirm("Delete this view note?")) return;
    const { dataset, session, embryo } = state.selected;
    try {
      await API.deleteViewNote(dataset, session, embryo, noteId, state.annotator);
      state.annotations.viewNotes = state.annotations.viewNotes.filter(
        (x) => x.id !== noteId
      );
      renderViewNotes();
      refreshAnnotationSummary();
    } catch (err) {
      setStatus(`Delete failed: ${err.message}`, true);
    }
  }

  async function patchViewNoteText(noteId, newText) {
    if (isViewOnly()) return;
    let note = newText.trim();
    let tag = null;
    const m = note.match(/^([A-Za-z][A-Za-z0-9_-]{0,23})\s*:\s*(.+)$/s);
    if (m) { tag = m[1].toLowerCase(); note = m[2].trim(); }
    if (!note) return deleteViewNote(noteId);
    const { dataset, session, embryo } = state.selected;
    try {
      await API.patchViewNote(dataset, session, embryo, noteId, {
        annotator: state.annotator, note, tag,
      });
      const local = state.annotations.viewNotes.find((x) => x.id === noteId);
      if (local) { local.note = note; local.tag = tag; }
      renderViewNotes();
    } catch (err) {
      setStatus(`Save failed: ${err.message}`, true);
    }
  }

  function renderViewNotes() {
    const list = document.getElementById("view-notes-list");
    const count = document.getElementById("view-notes-count");
    const addBtn = document.getElementById("view-note-add");
    if (!list || !count || !addBtn) return;
    const vns = state.annotations.viewNotes || [];
    const tp = state.selected.timepoint;
    addBtn.disabled = isViewOnly() || tp == null || !state.annotations.loaded;

    count.textContent = vns.length === 0 ? "(none)" :
      vns.length === 1 ? "1 view note" : `${vns.length} view notes`;

    if (vns.length === 0) {
      list.innerHTML = `<li class="vn-empty">No view notes yet for this embryo.</li>`;
      return;
    }
    // Sort: current tp first, then by tp ascending.
    const sorted = vns.slice().sort((a, b) => {
      if (a.timepoint === tp && b.timepoint !== tp) return -1;
      if (b.timepoint === tp && a.timepoint !== tp) return 1;
      return (a.timepoint - b.timepoint) || (a.id - b.id);
    });
    list.innerHTML = sorted.map((vn) => {
      const isCurrent = vn.timepoint === tp;
      const tagHtml = vn.tag ? `<span class="vn-tag">${escapeHtml(vn.tag)}</span>` : "";
      return `
        <li class="vn-item${isCurrent ? " current-tp" : ""}" data-id="${vn.id}">
          ${tagHtml}
          <span class="vn-tp">t=${vn.timepoint}</span>
          <span class="vn-text" data-edit="false">${escapeHtml(vn.note)}</span>
          <span class="vn-actions">
            <button class="vn-action vn-restore" type="button" title="Snap viewer to this exact view">↻</button>
            <button class="vn-action vn-edit" type="button" title="Edit (prefix with a tag like 'best: …')">✎</button>
            <button class="vn-action vn-del danger" type="button" title="Delete">✕</button>
          </span>
        </li>
      `;
    }).join("");

    list.querySelectorAll(".vn-restore").forEach((b) => {
      b.addEventListener("click", (e) => {
        const id = parseInt(e.target.closest(".vn-item").dataset.id, 10);
        restoreViewNote(id);
      });
    });
    list.querySelectorAll(".vn-del").forEach((b) => {
      b.addEventListener("click", (e) => {
        const id = parseInt(e.target.closest(".vn-item").dataset.id, 10);
        deleteViewNote(id);
      });
    });
    list.querySelectorAll(".vn-edit").forEach((b) => {
      b.addEventListener("click", (e) => {
        if (isViewOnly()) return readOnlyToast();
        const li = e.target.closest(".vn-item");
        const id = parseInt(li.dataset.id, 10);
        const span = li.querySelector(".vn-text");
        const original = span.textContent;
        // Reconstruct "tag: note" if there's a tag, so editing keeps the form.
        const vn = state.annotations.viewNotes.find((x) => x.id === id);
        const editing = vn?.tag ? `${vn.tag}: ${vn.note}` : vn ? vn.note : original;
        span.textContent = editing;
        span.dataset.edit = "true";
        span.contentEditable = "true";
        span.focus();
        const range = document.createRange();
        range.selectNodeContents(span);
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
        const commit = () => {
          span.contentEditable = "false";
          span.dataset.edit = "false";
          span.removeEventListener("blur", commit);
          span.removeEventListener("keydown", onKey);
          patchViewNoteText(id, span.textContent);
        };
        const onKey = (ev) => {
          if (ev.key === "Enter" && !ev.shiftKey) { ev.preventDefault(); commit(); }
          if (ev.key === "Escape") {
            span.textContent = original;
            span.contentEditable = "false";
            span.dataset.edit = "false";
            span.removeEventListener("blur", commit);
            span.removeEventListener("keydown", onKey);
          }
        };
        span.addEventListener("blur", commit);
        span.addEventListener("keydown", onKey);
      });
    });
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
    const enabled = state.annotations.loaded && tp != null && !isViewOnly();

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
    const gtLayer = document.getElementById("timeline-gt");
    const ticks = document.getElementById("timeline-ticks");
    const markers = document.getElementById("timeline-markers");
    const cursor = document.getElementById("timeline-cursor");
    if (!strip || !bands || !markers || !cursor) return;

    const enabled = state.timepoints.length > 0;
    strip.classList.toggle("disabled", !enabled);
    if (!enabled) {
      bands.innerHTML = ""; markers.innerHTML = "";
      if (gtLayer) gtLayer.innerHTML = "";
      if (ticks) ticks.innerHTML = "";
      cursor.style.display = "none";
      renderTimelineNotes();
      return;
    }

    const tps = state.timepoints;
    const N = tps.length;
    const xFor = (idx) => (N <= 1 ? 0 : (idx / (N - 1)) * 100);
    const widthFor = (idxStart, idxEnd) => xFor(idxEnd) - xFor(idxStart);

    // Ground-truth band (HF dataset only) — same band-from-transitions
    // walk as the user's stages, but driven by groundTruth.transitions
    // and styled as a thin reference strip.
    if (gtLayer) gtLayer.innerHTML = "";
    const gtTransitions = state.annotations.groundTruth.transitions || [];
    if (gtLayer && gtTransitions.length) {
      const stageAtGT = (tp) => {
        let best = null;
        for (const t of gtTransitions) {
          if (t.timepoint <= tp && (!best || t.timepoint > best.timepoint)) best = t;
        }
        return best ? best.stage : null;
      };
      let runStart = 0;
      let runStage = stageAtGT(tps[0]);
      for (let i = 1; i <= N; i++) {
        const cur = i < N ? stageAtGT(tps[i]) : null;
        if (i === N || cur !== runStage) {
          if (runStage) {
            const band = document.createElement("div");
            band.className = "tl-gt-band";
            band.style.left = xFor(runStart) + "%";
            band.style.width = widthFor(runStart, i - 1) + "%";
            band.style.background = STAGE_COLORS[runStage] || "#888";
            const gtAnnotator = state.annotations.groundTruth.annotator || "GT";
            band.title = `${gtAnnotator} (ground truth) — ${runStage}: t=${tps[runStart]} … t=${tps[i - 1]}`;
            gtLayer.appendChild(band);
          }
          runStart = i;
          runStage = cur;
        }
      }
      // Small marker triangles for each ground-truth transition.
      for (const t of gtTransitions) {
        const idx = tps.indexOf(t.timepoint);
        if (idx < 0) continue;
        const m = document.createElement("div");
        m.className = "tl-gt-marker";
        m.style.left = xFor(idx) + "%";
        m.style.setProperty("--marker-color", STAGE_COLORS[t.stage] || "#fff");
        m.title = `GT: ${t.stage} starts at t=${t.timepoint}`;
        gtLayer.appendChild(m);
      }
    }

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

    // Twitching event markers — distinct vertical bar. `mine` is bright;
    // `others` are dimmer so they don't compete with the active user's mark.
    const tw = state.annotations.twitching;
    if (tw && tw.mine) {
      const idx = tps.indexOf(tw.mine.timepoint);
      if (idx >= 0) {
        const m = document.createElement("div");
        m.className = "tl-twitch";
        m.style.left = xFor(idx) + "%";
        m.title = `twitching start (you) at t=${tw.mine.timepoint}`;
        markers.appendChild(m);
        const g = document.createElement("div");
        g.className = "tl-twitch-glyph";
        g.style.left = xFor(idx) + "%";
        g.textContent = "T";
        markers.appendChild(g);
      }
    }
    for (const o of (tw && tw.others) || []) {
      const idx = tps.indexOf(o.timepoint);
      if (idx < 0) continue;
      const m = document.createElement("div");
      m.className = "tl-twitch others";
      m.style.left = xFor(idx) + "%";
      m.title = `twitching start (${o.annotator}) at t=${o.timepoint}`;
      markers.appendChild(m);
    }

    renderTimelineTicks();
    renderTimelineNotes();
    renderTimelineOrientations();

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

  function renderTimelineOrientations() {
    const root = document.getElementById("timeline-bands");
    if (!root) return;
    const tps = state.timepoints;
    if (!tps.length) return;
    const xFor = (idx) => (tps.length <= 1 ? 0 : (idx / (tps.length - 1)) * 100);

    // Unreliable ranges as a translucent gray band layered over the stage band.
    for (const r of state.annotations.unreliableRanges || []) {
      const i0 = tps.findIndex((t) => t >= r.start_tp);
      const i1 = (() => {
        const idx = tps.findIndex((t) => t >= r.end_tp);
        return idx < 0 ? tps.length - 1 : idx;
      })();
      if (i0 < 0 || i1 < 0) continue;
      const band = document.createElement("div");
      band.className = "tl-unreliable";
      band.style.left = xFor(i0) + "%";
      band.style.width = (xFor(i1) - xFor(i0)) + "%";
      band.title = `unreliable: t=${r.start_tp}–${r.end_tp}`;
      root.appendChild(band);
    }

    // AP/DV markers as small colored dots above the band, paired vertically.
    const markers = document.getElementById("timeline-markers");
    for (const [tp, o] of state.annotations.orientations) {
      const idx = tps.indexOf(tp);
      if (idx < 0) continue;
      if (o.ap_dir) {
        const d = document.createElement("div");
        d.className = "tl-orient-ap";
        d.style.left = xFor(idx) + "%";
        d.title = `AP saved at t=${tp}`;
        markers.appendChild(d);
      }
      if (o.dv_dir) {
        const d = document.createElement("div");
        d.className = "tl-orient-dv";
        d.style.left = xFor(idx) + "%";
        d.title = `DV saved at t=${tp}`;
        markers.appendChild(d);
      }
    }

    // Pending unreliable-range start marker (visual feedback while user
    // is mid-range).
    if (state.unreliableMarking) {
      const idx = tps.indexOf(state.unreliableMarking.startTp);
      if (idx >= 0) {
        const d = document.createElement("div");
        d.className = "tl-pending-unreliable";
        d.style.left = xFor(idx) + "%";
        d.title = `unreliable range start at t=${state.unreliableMarking.startTp}`;
        markers.appendChild(d);
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
    const hasContext = tp != null && state.annotations.loaded;
    // disabled = totally inert (no tp loaded). readOnly = visible but not
    // editable (view-only mode). Keep them distinct so the viewed
    // annotator's note text is still readable + selectable.
    ta.disabled = !hasContext;
    ta.readOnly = isViewOnly();
    if (!hasContext) {
      ta.value = "";
      ta.placeholder = "optional morphology notes (auto-saves)";
      return;
    }
    const stored = state.annotations.notes.get(tp) || "";
    if (document.activeElement !== ta) {
      ta.value = stored;
    }
    ta.placeholder = isViewOnly()
      ? `no note from ${state.viewingAs} at this timepoint`
      : "optional morphology notes (auto-saves)";
  }

  function renderExcludeUI() {
    const cb = document.getElementById("exclude-input");
    const label = document.getElementById("exclude-label");
    if (!cb || !label) return;
    const enabled = state.annotations.loaded && !!state.selected.embryo && !isViewOnly();
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
