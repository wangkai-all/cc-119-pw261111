/* CSV 清理 — dashboard logic (plain ES2020, no build step). */
"use strict";

const state = {
  meta: null,
  upload: null,      // {upload_id, size, filename}
  inspect: null,     // {columns, sample_rows, ...}
  tasks: [],
  current: null,     // task detail
  timer: null,
  page: 0,
  pageSize: 50,
  shardView: "chart",
  polling: false,
};

const $ = (id) => document.getElementById(id);
const fmtInt = new Intl.NumberFormat("zh-CN");
const fmtBytes = (n) => {
  if (!n && n !== 0) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let i = 0, v = n;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v >= 100 || i === 0 ? Math.round(v) : v.toFixed(1)} ${units[i]}`;
};
const fmtDur = (ms) => {
  if (!ms) return "—";
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)} 秒`;
  const m = Math.floor(s / 60);
  return `${m} 分 ${Math.round(s % 60)} 秒`;
};
const esc = (s) => String(s ?? "");

function toast(message) {
  const el = $("toast");
  el.textContent = message;
  el.classList.add("show");
  clearTimeout(toast._t);
  toast._t = setTimeout(() => el.classList.remove("show"), 3200);
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: options.body && !(options.body instanceof Blob)
      ? { "Content-Type": "application/json" } : {},
    ...options,
  });
  if (options.raw) return res;
  let data = null;
  try { data = await res.json(); } catch (_) { /* empty */ }
  if (!res.ok) throw new Error((data && data.error) || `HTTP ${res.status}`);
  return data;
}

/* ------------------------------------------------------------------ */
/* bootstrap                                                           */
/* ------------------------------------------------------------------ */
async function boot() {
  wireEvents();
  try {
    state.meta = await api("/api/meta");
    const m = state.meta.machine;
    $("machine").textContent = `${m.cpus} 核 · 峰值 RSS ${m.peak_rss_mb} MiB`;
    $("chunkRows").value = state.meta.defaults.chunk_rows;
    $("chunkBytes").value = state.meta.defaults.chunk_bytes;
    $("fanIn").value = state.meta.defaults.fan_in;
  } catch (err) {
    toast(`无法读取服务信息：${err.message}`);
  }
  refreshThemeButton();
  await loadTasks();
  startPolling();
}

function wireEvents() {
  $("themeBtn").addEventListener("click", toggleTheme);
  $("refreshBtn").addEventListener("click", () => loadTasks(true));

  const drop = $("dropZone");
  const input = $("fileInput");
  drop.addEventListener("click", () => input.click());
  drop.addEventListener("keydown", (e) => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); input.click(); }
  });
  input.addEventListener("change", () => {
    if (input.files.length) uploadFile(input.files[0]);
  });
  for (const evt of ["dragenter", "dragover"]) {
    drop.addEventListener(evt, (e) => { e.preventDefault(); drop.classList.add("dragover"); });
  }
  for (const evt of ["dragleave", "drop"]) {
    drop.addEventListener(evt, (e) => { e.preventDefault(); drop.classList.remove("dragover"); });
  }
  drop.addEventListener("drop", (e) => {
    const file = e.dataTransfer.files && e.dataTransfer.files[0];
    if (file) uploadFile(file);
  });

  $("encoding").addEventListener("change", scheduleInspect);
  $("delimiter").addEventListener("change", scheduleInspect);
  $("headerMode").addEventListener("change", () => {
    $("customHeaderWrap").hidden = $("headerMode").value !== "custom";
    scheduleInspect();
  });
  $("inspectBtn").addEventListener("click", runInspect);
  $("startBtn").addEventListener("click", createTask);
  $("dupPolicy").addEventListener("change", updateRuleNote);
  $("sortType").addEventListener("change", updateRuleNote);
  $("businessKey").addEventListener("input", updateRuleNote);

  $("pauseBtn").addEventListener("click", () => control("pause"));
  $("resumeBtn").addEventListener("click", () => control("resume"));
  $("cancelBtn").addEventListener("click", () => control("cancel"));
  $("deleteBtn").addEventListener("click", deleteCurrent);

  $("chartViewBtn").addEventListener("click", () => setShardView("chart"));
  $("tableViewBtn").addEventListener("click", () => setShardView("table"));
  $("prevPage").addEventListener("click", () => { if (state.page > 0) { state.page--; loadPreview(); } });
  $("nextPage").addEventListener("click", () => { state.page++; loadPreview(); });
  $("searchBtn").addEventListener("click", runSearch);
  $("searchInput").addEventListener("keydown", (e) => { if (e.key === "Enter") runSearch(); });
  window.addEventListener("resize", debounce(() => {
    if (state.current && state.shardView === "chart") renderShardChart(state.current);
  }, 150));
}

function debounce(fn, ms) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}
const scheduleInspect = debounce(() => { if (state.upload) runInspect(); }, 250);

/* ------------------------------------------------------------------ */
/* theme                                                               */
/* ------------------------------------------------------------------ */
function toggleTheme() {
  const current = document.documentElement.getAttribute("data-theme");
  const next = current === "dark" ? "light" : "dark";
  document.documentElement.setAttribute("data-theme", next);
  localStorage.setItem("csvclean-theme", next);
  refreshThemeButton();
  if (state.current && state.shardView === "chart") renderShardChart(state.current);
}
function refreshThemeButton() {
  const saved = localStorage.getItem("csvclean-theme");
  if (saved) document.documentElement.setAttribute("data-theme", saved);
  const dark = document.documentElement.getAttribute("data-theme") === "dark";
  $("themeBtn").textContent = dark ? "☀" : "◐";
  $("themeBtn").setAttribute("aria-label", dark ? "切换到亮色" : "切换到暗色");
}

/* ------------------------------------------------------------------ */
/* upload + inspect                                                    */
/* ------------------------------------------------------------------ */
async function uploadFile(file) {
  const status = $("uploadStatus");
  status.textContent = `正在上传 ${file.name}…`;
  try {
    const res = await fetch("/api/uploads", {
      method: "POST",
      headers: { "X-Filename": file.name },
      body: file,
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    state.upload = data;
    $("fileName").textContent = `${file.name} · ${fmtBytes(data.size)}`;
    $("uploadStatus").textContent = "上传完成，正在预览列…";
    await runInspect();
  } catch (err) {
    status.textContent = `上传失败：${err.message}`;
    toast(`上传失败：${err.message}`);
  }
}

function delimiterValue() {
  const v = $("delimiter").value;
  return v === "\\t" ? "\t" : v;
}

async function runInspect() {
  if (!state.upload) return;
  $("inspectHint").textContent = "读取中…";
  try {
    const data = await api("/api/inspect", {
      method: "POST",
      body: JSON.stringify({
        upload_id: state.upload.upload_id,
        encoding: $("encoding").value,
        delimiter: delimiterValue(),
        header_mode: $("headerMode").value === "custom" ? "present" : $("headerMode").value,
      }),
    });
    state.inspect = data;
    $("inspectHint").textContent = `${data.columns.length} 列 · ${fmtBytes(data.size)}`;
    renderColumnPreview(data);
    fillColumnSelectors(data.columns);
    $("startBtn").disabled = false;
  } catch (err) {
    state.inspect = null;
    $("inspectHint").textContent = `无法预览：${err.message}`;
    $("columnPreview").hidden = true;
    $("startBtn").disabled = true;
  }
}

function renderColumnPreview(info) {
  $("columnPreview").hidden = false;
  const chips = $("columnChips");
  chips.replaceChildren();
  for (const col of info.columns) {
    const chip = document.createElement("span");
    chip.className = "chip";
    chip.textContent = col;
    chips.appendChild(chip);
  }
  const table = $("sampleTable");
  table.replaceChildren();
  const thead = document.createElement("thead");
  const tr = document.createElement("tr");
  for (const col of info.columns) {
    const th = document.createElement("th");
    th.textContent = col;
    tr.appendChild(th);
  }
  thead.appendChild(tr);
  table.appendChild(thead);
  const tbody = document.createElement("tbody");
  for (const row of info.sample_rows.slice(0, 5)) {
    const trb = document.createElement("tr");
    for (const col of info.columns) {
      const td = document.createElement("td");
      const value = row[col];
      td.textContent = value === "" || value == null ? "∅" : value;
      if (value === "" || value == null) td.className = "muted";
      trb.appendChild(td);
    }
    tbody.appendChild(trb);
  }
  table.appendChild(tbody);
}

function fillColumnSelectors(columns) {
  const sortSel = $("sortBy");
  const timeSel = $("updatedAt");
  const prevSort = sortSel.value;
  const prevTime = timeSel.value;
  sortSel.replaceChildren();
  timeSel.replaceChildren();
  for (const col of columns) {
    const opt = document.createElement("option");
    opt.value = col; opt.textContent = col;
    sortSel.appendChild(opt);
  }
  const none = document.createElement("option");
  none.value = ""; none.textContent = "（不使用）";
  timeSel.appendChild(none);
  for (const col of columns) {
    const opt = document.createElement("option");
    opt.value = col; opt.textContent = col;
    timeSel.appendChild(opt);
  }
  sortSel.value = columns.includes(prevSort) ? prevSort : columns[0];
  timeSel.value = columns.includes(prevTime) ? prevTime
    : (columns.find((c) => /time|date|updated|时间/i.test(c)) || "");
  $("businessKey").value = columns[0] || "";
  updateRuleNote();
}

function updateRuleNote() {
  const policy = $("dupPolicy").value;
  const key = $("businessKey").value.trim() || "（排序字段）";
  const type = $("sortType").value;
  const parts = [];
  if (policy === "keep") {
    parts.push("保留全部记录，只做排序。");
  } else if (policy === "latest") {
    const time = $("updatedAt").value;
    parts.push(time
      ? `业务键 ${key} 重复时保留 ${time} 最新的一条；时间相同按原始行号取靠前的。`
      : "按更新时间去重需要选择更新时间列。");
  } else {
    parts.push(`业务键 ${key} 重复时保留最早出现的一条。`);
  }
  if (type === "auto") parts.push("自动模式识别数字与日期，金额按数值比较。");
  if (type === "number") parts.push("金额按十进制数值比较，不会出现 9.10 排在 10.00 之后。");
  $("ruleNote").textContent = parts.join(" ");
}

/* ------------------------------------------------------------------ */
/* create task                                                         */
/* ------------------------------------------------------------------ */
async function createTask() {
  if (!state.upload) { toast("请先上传文件"); return; }
  const payload = {
    upload_id: state.upload.upload_id,
    encoding: $("encoding").value,
    delimiter: delimiterValue(),
    header_mode: $("headerMode").value,
    header_names: $("headerMode").value === "custom"
      ? $("customHeader").value.split(",").map((s) => s.trim()).filter(Boolean)
      : [],
    name: $("taskName").value.trim(),
    chunk_rows: Number($("chunkRows").value),
    chunk_bytes: Number($("chunkBytes").value),
    fan_in: Number($("fanIn").value),
    rules: {
      sort_by: $("sortBy").value,
      sort_dir: $("sortDir").value,
      nulls: $("nulls").value,
      business_key: $("businessKey").value.trim() || $("sortBy").value,
      updated_at: $("updatedAt").value || null,
      dup_policy: $("dupPolicy").value,
      sort_type: $("sortType").value,
    },
  };
  $("startBtn").disabled = true;
  try {
    const res = await api("/api/tasks", { method: "POST", body: JSON.stringify(payload) });
    const detail = await api(`/api/tasks/${res.task_id}`);
    state.tasks = state.tasks.filter((t) => t.id !== res.task_id);
    state.tasks.unshift(detail);
    selectTask(res.task_id);
    await loadTasks();
    toast(res.reused ? "已复用相同输入与规则的任务" : "任务已创建");
  } catch (err) {
    toast(`创建失败：${err.message}`);
  } finally {
    $("startBtn").disabled = !state.inspect;
  }
}

/* ------------------------------------------------------------------ */
/* task list + polling                                                 */
/* ------------------------------------------------------------------ */
async function loadTasks(notify = false) {
  try {
    const data = await api("/api/tasks");
    state.tasks = data.tasks;
    renderTaskList();
    if (notify) toast("任务列表已刷新");
  } catch (err) {
    toast(`刷新失败：${err.message}`);
  }
}

const STATUS_LABEL = {
  queued: "排队中", running: "运行中", pausing: "正在暂停", paused: "已暂停",
  completed: "已完成", failed: "失败", cancelled: "已取消",
  interrupted: "已中断，可继续", input_changed: "输入已变化",
};

function renderTaskList() {
  const list = $("taskList");
  list.replaceChildren();
  $("taskEmpty").hidden = state.tasks.length > 0;
  for (const task of state.tasks) {
    const li = document.createElement("li");
    li.className = "task-item";
    li.tabIndex = 0;
    if (state.current && state.current.id === task.id) li.setAttribute("aria-current", "true");
    const name = document.createElement("span");
    name.className = "name";
    name.textContent = `#${task.id} ${task.name}`;
    const meta = document.createElement("span");
    meta.className = "meta";
    const pill = document.createElement("span");
    pill.className = `pill ${task.status}`;
    pill.textContent = STATUS_LABEL[task.status] || task.status;
    meta.appendChild(pill);
    const counts = document.createElement("span");
    counts.textContent = task.status === "completed"
      ? `${fmtInt.format(task.output_rows)} 行`
      : `${fmtInt.format(task.records_read)} / ${fmtInt.format(task.good_rows + task.bad_rows)}`;
    meta.appendChild(counts);
    li.append(name, meta);
    li.addEventListener("click", () => selectTask(task.id));
    li.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); selectTask(task.id); }
    });
    list.appendChild(li);
  }
}

async function selectTask(id) {
  state.page = 0;
  $("taskCard").hidden = false;
  $("limitCard").hidden = true;
  $("newCard").scrollIntoView({ block: "start", behavior: "smooth" });
  await pollTask(id);
}

function startPolling() {
  clearInterval(state.timer);
  state.timer = setInterval(async () => {
    if (state.polling) return;
    if (state.current) {
      const active = ["running", "pausing", "queued", "paused"].includes(state.current.status);
      if (active) await pollTask(state.current.id, true);
      else if (state.tasks.some((t) => ["running", "pausing", "queued"].includes(t.status))) {
        await loadTasks();
      }
      return;
    }
    if (state.tasks.some((t) => ["running", "pausing", "queued"].includes(t.status))) {
      await loadTasks();
    }
  }, 900);
}

async function pollTask(id, quiet = false) {
  state.polling = true;
  try {
    const detail = await api(`/api/tasks/${id}`);
    const wasCompleted = state.current && state.current.id === id
      && state.current.status === "completed";
    state.current = detail;
    renderTask(detail);
    const idx = state.tasks.findIndex((t) => t.id === id);
    if (idx >= 0) { state.tasks[idx] = { ...state.tasks[idx], ...detail }; }
    renderTaskList();
    if (detail.status === "completed" && !wasCompleted) {
      await loadPreview();
      $("resultBlock").hidden = false;
    }
    if (detail.status !== "completed") $("resultBlock").hidden = true;
  } catch (err) {
    if (!quiet) toast(`读取任务失败：${err.message}`);
  } finally {
    state.polling = false;
  }
}

/* ------------------------------------------------------------------ */
/* render task                                                         */
/* ------------------------------------------------------------------ */
function renderTask(task) {
  const running = task.running;
  const status = task.status;
  $("taskTitle").textContent = `#${task.id} ${task.name}`;
  $("taskSub").textContent = `${task.input.encoding} · 分隔符 ${JSON.stringify(
    task.input.delimiter === "\t" ? "\\t" : task.input.delimiter)} · ` +
    `${fmtBytes(task.input.size)} · 块 ${fmtInt.format(task.config.chunk_rows)} 行 / ` +
    `${fmtBytes(task.config.chunk_bytes)} · 扇入 ${task.config.fan_in}`;
  const pill = $("statusPill");
  pill.className = `pill ${status}`;
  pill.textContent = STATUS_LABEL[status] || status;
  $("stageText").textContent = stageLabel(task);
  $("elapsedText").textContent = task.progress.elapsed_ms
    ? `用时 ${fmtDur(task.progress.elapsed_ms)}` : "";
  const err = $("taskError");
  err.hidden = !task.error;
  err.textContent = task.error || "";

  $("pauseBtn").hidden = !(running && status === "running");
  $("resumeBtn").hidden = !(["paused", "interrupted", "pausing"].includes(status)
    || (!running && ["queued"].includes(status)));
  $("cancelBtn").hidden = !running;
  $("deleteBtn").hidden = running;

  const p = task.progress;
  const total = task.input.size || 0;
  const readPct = total ? Math.min(100, (p.bytes_read / total) * 100) : 0;
  setMeter("read", readPct, `${fmtBytes(p.bytes_read)} / ${fmtBytes(total)}`
    + ` · 已读 ${fmtInt.format(p.records_read)} 条记录`, status === "completed");
  $("readVal").textContent = `${readPct.toFixed(0)}%`;

  const shardCount = p.shards_written || 0;
  setMeter("shard", shardCount ? 100 : 0,
    shardCount
      ? `${fmtInt.format(shardCount)} 个分片 · ${fmtBytes(p.shard_bytes_written)} · 最大块分片 ${fmtBytes(p.peak_shard_bytes)}`
      : "尚未生成分片", status === "completed");
  $("shardVal").textContent = shardCount ? `${fmtInt.format(shardCount)} 个` : "0";

  const mergeTotal = p.merge_parts_total || 0;
  const mergeDone = p.merge_parts_done || 0;
  const mergePct = status === "completed" ? 100 : (mergeTotal ? (mergeDone / mergeTotal) * 100 : 0);
  const mergeLabel = mergeTotal
    ? `${fmtInt.format(mergeDone)} / ${fmtInt.format(mergeTotal)} 个分片已归并`
    : "等待开始";
  setMeter("merge", mergePct, mergeLabel, status === "completed");

  renderStats(task);
  renderShardTable(task);
  if (state.shardView === "chart") renderShardChart(task);
  renderBadSamples(task);
  $("downloadClean").href = `/api/tasks/${task.id}/download`;
  $("downloadBad").href = `/api/tasks/${task.id}/download?kind=bad`;
  $("downloadClean").setAttribute("aria-disabled", status !== "completed");
  $("downloadBad").setAttribute("aria-disabled", status !== "completed");
  $("rawJson").textContent = JSON.stringify({
    config: task.config, rules: task.rules, progress: task.progress,
    shards: task.shards, input_sha256: task.input.sha256,
  }, null, 2);
}

function stageLabel(task) {
  const map = {
    starting: "准备中", recover: "核对已有临时分片", scan: "读取输入并按块排序",
    dedupe: "按业务键合并去重", merge: "多路归并", final: "写出结果文件",
    done: "已完成", failed: "失败", cancelled: "已取消", paused: "已暂停",
  };
  return map[task.stage] || task.stage || "";
}

function setMeter(key, pct, foot, done) {
  const fill = $(`${key}Fill`);
  fill.style.width = `${Math.max(0, Math.min(100, pct))}%`;
  fill.className = "meter-fill" + (done ? " done" : "");
  $(`${key}Foot`).textContent = foot;
  const meter = $(`${key}Meter`);
  meter.setAttribute("aria-valuenow", String(Math.round(pct)));
  $(`${key}Val`).textContent = key === "shard" ? $(`${key}Val`).textContent
    : `${pct.toFixed(0)}%`;
}

function renderStats(task) {
  const p = task.progress;
  const stats = [
    ["已读记录", fmtInt.format(p.records_read), `${fmtInt.format(p.physical_lines)} 物理行`],
    ["有效记录", fmtInt.format(p.good_rows), "已按规则读取"],
    ["坏行", fmtInt.format(p.bad_rows), "已隔离，保留原位置"],
    ["去重移除", fmtInt.format(p.dup_removed), task.rules.dup_policy === "keep" ? "保留全部" : "按业务键"],
    ["输出行数", fmtInt.format(p.output_rows), fmtBytes(p.output_bytes)],
    ["临时分片", fmtInt.format(p.shards_written), fmtBytes(p.shard_bytes_written)],
    ["峰值 RSS", `${p.peak_rss_mb || 0} MiB`, "进程内存"],
  ];
  const box = $("stats");
  box.replaceChildren();
  for (const [label, value, hint] of stats) {
    const el = document.createElement("div");
    el.className = "stat";
    const l = document.createElement("span"); l.className = "label"; l.textContent = label;
    const v = document.createElement("span"); v.className = "value"; v.textContent = value;
    const h = document.createElement("span"); h.className = "hint"; h.textContent = hint;
    el.append(l, v, h);
    box.appendChild(el);
  }
}

/* ------------------------------------------------------------------ */
/* shard table + chart                                                 */
/* ------------------------------------------------------------------ */
const STAGE_COLOR = {
  scan: "var(--series-1)",
  value: "var(--series-2)",
  merge: "var(--series-3)",
};
const STAGE_NAME = { scan: "扫描分片", value: "去重后分片", merge: "归并中间分片" };

function renderShardTable(task) {
  const tbody = $("shardTable").querySelector("tbody");
  tbody.replaceChildren();
  const shards = task.shards || [];
  $("shardEmpty").hidden = shards.length > 0;
  for (const shard of shards.slice(-200)) {
    const tr = document.createElement("tr");
    const cells = [
      STAGE_NAME[shard.dataset] || shard.dataset,
      shard.level ? `L${shard.level}-${shard.part}` : String(shard.part),
      fmtInt.format(shard.rows),
      fmtInt.format(shard.bad_rows || 0),
      fmtBytes(shard.bytes),
    ];
    cells.forEach((value, i) => {
      const td = document.createElement("td");
      td.textContent = value;
      if (i >= 2) td.className = "num";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  }
}

function setShardView(view) {
  state.shardView = view;
  $("chartViewBtn").setAttribute("aria-pressed", String(view === "chart"));
  $("tableViewBtn").setAttribute("aria-pressed", String(view === "table"));
  $("shardChart").hidden = view !== "chart";
  $("shardTableWrap").hidden = view !== "table";
  $("shardLegend").hidden = view !== "chart";
  if (view === "chart" && state.current) renderShardChart(state.current);
}

/**
 * Column chart of the last N temporary shards by byte size, colored by stage.
 * One y-axis, 24px-max bars with 4px rounded caps, 2px surface gaps,
 * a direct label on the largest bar only, hover + focus tooltips, legend.
 */
function renderShardChart(task) {
  const host = $("shardChart");
  host.replaceChildren();
  $("shardLegend").hidden = false;
  const all = (task.shards || []).filter((s) => s.bytes > 0);
  if (!all.length) {
    const empty = document.createElement("p");
    empty.className = "empty-hint";
    empty.textContent = "任务开始后会显示每个临时分片的大小。";
    host.appendChild(empty);
    renderLegend([]);
    return;
  }
  const MAX_BARS = 40;
  const shards = all.slice(-MAX_BARS);
  const stages = [...new Set(shards.map((s) => s.dataset))];
  renderLegend(stages);

  const width = host.clientWidth || 640;
  const pad = { top: 16, right: 12, bottom: 30, left: 56 };
  const innerW = Math.max(120, width - pad.left - pad.right);
  const innerH = 150;
  const height = innerH + pad.top + pad.bottom;
  const max = Math.max(...shards.map((s) => s.bytes));
  const niceMax = niceCeil(max);

  const svgNS = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(svgNS, "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-label",
    `临时分片大小：最近 ${shards.length} 个分片，最大 ${fmtBytes(max)}`);

  const y = (v) => pad.top + innerH - (v / niceMax) * innerH;
  const ticks = 4;
  for (let i = 0; i <= ticks; i++) {
    const value = (niceMax / ticks) * i;
    const gy = y(value);
    const line = document.createElementNS(svgNS, "line");
    line.setAttribute("x1", pad.left);
    line.setAttribute("x2", pad.left + innerW);
    line.setAttribute("y1", gy);
    line.setAttribute("y2", gy);
    line.setAttribute("stroke", i === 0 ? "var(--axis)" : "var(--grid)");
    line.setAttribute("stroke-width", "1");
    svg.appendChild(line);
    const label = document.createElementNS(svgNS, "text");
    label.setAttribute("x", pad.left - 8);
    label.setAttribute("y", gy + 3);
    label.setAttribute("text-anchor", "end");
    label.textContent = fmtBytes(value);
    svg.appendChild(label);
  }

  const slot = innerW / shards.length;
  const barW = Math.max(2, Math.min(24, slot - 2));
  const maxIndex = shards.indexOf(shards.reduce((a, b) => (a.bytes >= b.bytes ? a : b)));

  const tip = $("tooltip");
  const showTip = (evt, shard) => {
    tip.replaceChildren();
    const val = document.createElement("span");
    val.className = "val";
    val.textContent = fmtBytes(shard.bytes);
    const sub = document.createElement("span");
    sub.className = "sub";
    sub.textContent = `${STAGE_NAME[shard.dataset] || shard.dataset}` +
      `${shard.level ? " L" + shard.level : ""} #${shard.part} · ` +
      `${fmtInt.format(shard.rows)} 行` +
      (shard.bad_rows ? ` · ${fmtInt.format(shard.bad_rows)} 坏行` : "");
    tip.append(val, sub);
    tip.hidden = false;
    const rect = evt.currentTarget.getBoundingClientRect();
    const tw = tip.offsetWidth, th = tip.offsetHeight;
    let x = rect.left + rect.width / 2 - tw / 2;
    let yy = rect.top - th - 8;
    if (yy < 4) yy = rect.bottom + 8;
    tip.style.left = `${Math.max(4, Math.min(window.innerWidth - tw - 4, x))}px`;
    tip.style.top = `${yy}px`;
  };
  const hideTip = () => { tip.hidden = true; };

  shards.forEach((shard, i) => {
    const x = pad.left + i * slot + (slot - barW) / 2;
    const top = y(shard.bytes);
    const h = pad.top + innerH - top;
    const r = Math.min(4, h, barW / 2);
    // 4px rounded data-end, square baseline
    const path = document.createElementNS(svgNS, "path");
    path.setAttribute("d", roundedTop(x, top, barW, h, r));
    path.setAttribute("fill", STAGE_COLOR[shard.dataset] || "var(--series-1)");
    path.setAttribute("class", "bar");
    path.setAttribute("tabindex", "0");
    path.setAttribute("role", "listitem");
    path.setAttribute("aria-label",
      `${STAGE_NAME[shard.dataset] || shard.dataset} ${shard.part}：${fmtBytes(shard.bytes)}，${shard.rows} 行`);
    path.addEventListener("pointerenter", (e) => showTip(e, shard));
    path.addEventListener("pointermove", (e) => showTip(e, shard));
    path.addEventListener("pointerleave", hideTip);
    path.addEventListener("focus", (e) => showTip(e, shard));
    path.addEventListener("blur", hideTip);
    svg.appendChild(path);

    if (i === maxIndex) {
      const label = document.createElementNS(svgNS, "text");
      label.setAttribute("class", "bar-label");
      label.setAttribute("x", x + barW / 2);
      label.setAttribute("y", Math.max(10, top - 6));
      label.setAttribute("text-anchor", "middle");
      label.textContent = fmtBytes(shard.bytes);
      svg.appendChild(label);
    }
  });

  const first = shards[0], last = shards[shards.length - 1];
  const axis = document.createElementNS(svgNS, "text");
  axis.setAttribute("x", pad.left);
  axis.setAttribute("y", pad.top + innerH + 18);
  axis.textContent = `分片 ${first.part}…${last.part}（按生成顺序，最近 ${shards.length} 个）`;
  svg.appendChild(axis);

  host.appendChild(svg);
}

function renderLegend(stages) {
  const legend = $("shardLegend");
  legend.replaceChildren();
  for (const stage of stages) {
    const item = document.createElement("span");
    item.className = "key";
    const sw = document.createElement("span");
    sw.className = "swatch";
    sw.style.background = STAGE_COLOR[stage] || "var(--series-1)";
    const label = document.createElement("span");
    label.textContent = STAGE_NAME[stage] || stage;
    item.append(sw, label);
    legend.appendChild(item);
  }
}

function roundedTop(x, y, w, h, r) {
  if (h <= 0) return "";
  const rr = Math.min(r, h);
  return `M${x},${y + h} L${x},${y + rr} Q${x},${y} ${x + rr},${y} ` +
    `L${x + w - rr},${y} Q${x + w},${y} ${x + w},${y + rr} L${x + w},${y + h} Z`;
}

function niceCeil(value) {
  if (value <= 0) return 1;
  const exp = Math.pow(10, Math.floor(Math.log10(value)));
  const frac = value / exp;
  const nice = frac <= 1 ? 1 : frac <= 2 ? 2 : frac <= 5 ? 5 : 10;
  return nice * exp;
}

/* ------------------------------------------------------------------ */
/* bad samples                                                         */
/* ------------------------------------------------------------------ */
function renderBadSamples(task) {
  const tbody = $("badTable").querySelector("tbody");
  tbody.replaceChildren();
  const samples = task.bad_samples || [];
  $("badEmpty").hidden = samples.length > 0;
  $("badCountHint").textContent = task.progress.bad_rows
    ? `共 ${fmtInt.format(task.progress.bad_rows)} 行，显示前 ${samples.length} 条`
    : "";
  for (const sample of samples) {
    const tr = document.createElement("tr");
    const tdLine = document.createElement("td");
    tdLine.className = "num";
    tdLine.textContent = sample.end_line && sample.end_line !== sample.start_line
      ? `${sample.start_line}–${sample.end_line}` : String(sample.start_line);
    const tdReason = document.createElement("td");
    tdReason.textContent = sample.reason;
    const tdRaw = document.createElement("td");
    tdRaw.className = "raw";
    tdRaw.textContent = sample.raw;
    tr.append(tdLine, tdReason, tdRaw);
    tbody.appendChild(tr);
  }
}

/* ------------------------------------------------------------------ */
/* result preview + search                                             */
/* ------------------------------------------------------------------ */
async function loadPreview() {
  if (!state.current) return;
  try {
    const data = await api(`/api/tasks/${state.current.id}/preview?offset=` +
      `${state.page * state.pageSize}&limit=${state.pageSize}`);
    renderResultTable(data);
  } catch (err) {
    toast(`预览失败：${err.message}`);
  }
}

function renderResultTable(data) {
  const table = $("resultTable");
  table.replaceChildren();
  const thead = document.createElement("thead");
  const tr = document.createElement("tr");
  for (const col of data.columns) {
    const th = document.createElement("th");
    th.textContent = col;
    tr.appendChild(th);
  }
  thead.appendChild(tr);
  table.appendChild(thead);
  const tbody = document.createElement("tbody");
  for (const row of data.rows) {
    const r = document.createElement("tr");
    for (const cell of row) {
      const td = document.createElement("td");
      td.textContent = cell === "" ? "∅" : cell;
      if (cell === "") td.className = "muted";
      r.appendChild(td);
    }
    tbody.appendChild(r);
  }
  table.appendChild(tbody);
  $("pageInfo").textContent = `第 ${state.page + 1} 页 · ${data.rows.length} 行` +
    (data.has_more ? "（还有更多）" : "（最后一页）");
  $("nextPage").disabled = !data.has_more;
  $("prevPage").disabled = state.page === 0;
  $("searchInfo").textContent = "";
}

async function runSearch() {
  if (!state.current) return;
  const q = $("searchInput").value.trim();
  if (!q) { loadPreview(); return; }
  try {
    const data = await api(`/api/tasks/${state.current.id}/search?q=${encodeURIComponent(q)}`);
    const table = $("resultTable");
    table.replaceChildren();
    const thead = document.createElement("thead");
    const tr = document.createElement("tr");
    const thNo = document.createElement("th");
    thNo.textContent = "行号";
    tr.appendChild(thNo);
    for (const col of data.columns) {
      const th = document.createElement("th");
      th.textContent = col;
      tr.appendChild(th);
    }
    thead.appendChild(tr);
    table.appendChild(thead);
    const tbody = document.createElement("tbody");
    for (const hit of data.hits) {
      const r = document.createElement("tr");
      const tdNo = document.createElement("td");
      tdNo.className = "num";
      tdNo.textContent = String(hit.row);
      r.appendChild(tdNo);
      for (const cell of hit.fields) {
        const td = document.createElement("td");
        td.textContent = cell;
        r.appendChild(td);
      }
      tbody.appendChild(r);
    }
    table.appendChild(tbody);
    $("searchInfo").textContent = `“${data.query}” 命中 ${data.hits.length} 行` +
      `（已扫描 ${fmtInt.format(data.scanned)} 行${data.truncated ? "，结果已截断" : ""}）`;
  } catch (err) {
    toast(`搜索失败：${err.message}`);
  }
}

/* ------------------------------------------------------------------ */
/* controls                                                            */
/* ------------------------------------------------------------------ */
async function control(action) {
  if (!state.current) return;
  try {
    await api(`/api/tasks/${state.current.id}/${action}`, { method: "POST" });
    toast({ pause: "已请求暂停", resume: "已继续", cancel: "已取消，正在清理临时文件" }[action]);
    await pollTask(state.current.id, true);
  } catch (err) {
    toast(`操作失败：${err.message}`);
  }
}

async function deleteCurrent() {
  if (!state.current) return;
  if (!confirm("删除任务及其临时文件？已下载的结果不受影响。")) return;
  try {
    await api(`/api/tasks/${state.current.id}`, { method: "DELETE" });
    state.current = null;
    $("taskCard").hidden = true;
    await loadTasks();
    toast("任务已删除");
  } catch (err) {
    toast(`删除失败：${err.message}`);
  }
}

boot();
