// 无构建前端：轮询 API，自己用 canvas 画折线，避免为了一个看板引入打包器。
const REFRESH_MS = 2000;
const WINDOW_S = 600;
const BUCKET_S = 10;
const CHART_METRICS = ["temp", "hum", "vib", "rssi"];

const state = { devices: [], series: new Map(), alerts: [], errors: 0 };

const $ = (id) => document.getElementById(id);

function fmtTime(ts) {
  const d = new Date(ts);
  return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}:${String(d.getSeconds()).padStart(2, "0")}`;
}

function fmtAge(sec) {
  if (sec < 60) return `${sec.toFixed(0)}s 前`;
  if (sec < 3600) return `${(sec / 60).toFixed(0)}m 前`;
  return `${(sec / 3600).toFixed(1)}h 前`;
}

async function getJSON(url) {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`${url} → ${res.status}`);
  return res.json();
}

function drawChart(canvas, points) {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth * dpr;
  const h = canvas.clientHeight * dpr;
  if (canvas.width !== w || canvas.height !== h) { canvas.width = w; canvas.height = h; }
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, w, h);
  if (!points.length) return;

  const pad = 3 * dpr;
  let lo = Infinity, hi = -Infinity;
  for (const p of points) { lo = Math.min(lo, p.min); hi = Math.max(hi, p.max); }
  if (hi - lo < 1e-6) { lo -= 1; hi += 1; }
  const span = hi - lo;

  ctx.strokeStyle = "#1d2b39";
  ctx.lineWidth = 1;
  for (let i = 1; i < 3; i++) {
    const y = pad + (h - 2 * pad) * i / 3;
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
  }

  const step = points.length > 1 ? (w - 2 * pad) / (points.length - 1) : 0;
  ctx.beginPath();
  points.forEach((p, i) => {
    const x = pad + i * step;
    const y = pad + (h - 2 * pad) * (1 - (p.avg - lo) / span);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.strokeStyle = "#45d1a8";
  ctx.lineWidth = 1.6 * dpr;
  ctx.stroke();

  // min/max 带：让"平均值平滑"不至于掩盖抖动
  ctx.fillStyle = "rgba(69, 209, 168, .10)";
  ctx.beginPath();
  points.forEach((p, i) => {
    const x = pad + i * step;
    const y = pad + (h - 2 * pad) * (1 - (p.max - lo) / span);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  for (let i = points.length - 1; i >= 0; i--) {
    const p = points[i];
    const x = pad + i * step;
    const y = pad + (h - 2 * pad) * (1 - (p.min - lo) / span);
    ctx.lineTo(x, y);
  }
  ctx.closePath();
  ctx.fill();

  const last = points[points.length - 1];
  const y = pad + (h - 2 * pad) * (1 - (last.avg - lo) / span);
  ctx.fillStyle = "#f2d36b";
  ctx.beginPath();
  ctx.arc(pad + (points.length - 1) * step, y, 2.4 * dpr, 0, Math.PI * 2);
  ctx.fill();
}

function deviceCard(dev) {
  const el = document.createElement("div");
  el.className = "device";
  const ageClass = dev.age_s > 120 ? "device-age stale" : "device-age";
  el.innerHTML = `
    <div class="device-head">
      <span class="device-id">${dev.device_id}</span>
      <span class="${ageClass}">${fmtAge(dev.age_s)} · ${dev.samples} 条</span>
    </div>
    <div class="charts"></div>`;
  const charts = el.querySelector(".charts");
  for (const metric of CHART_METRICS) {
    if (!dev.metrics.includes(metric)) continue;
    const wrap = document.createElement("div");
    wrap.className = "chart";
    const last = state.series.get(`${dev.device_id}|${metric}`)?.at(-1);
    wrap.innerHTML = `
      <div class="chart-label"><span>${metric}</span>
        <b>${last ? last.avg.toFixed(2) : "–"}</b></div>
      <canvas data-device="${dev.device_id}" data-metric="${metric}"></canvas>`;
    charts.appendChild(wrap);
  }
  return el;
}

async function refresh() {
  try {
    const [stats, devices] = await Promise.all([getJSON("/api/stats"), getJSON("/api/devices")]);
    const [alerts, rules] = await Promise.all([
      getJSON("/api/alerts?limit=12"), getJSON("/api/rules"),
    ]);
    state.errors = 0;
    state.devices = devices;
    state.alerts = alerts;

    $("live-dot").classList.toggle("on", !!stats.mqtt?.connected || stats.ingest.accepted > 0);
    $("s-devices").textContent = stats.devices;
    $("s-samples").textContent = stats.samples;
    $("s-rate").textContent = stats.samples_per_min;
    const active = $("s-active");
    active.textContent = `${stats.active_alerts} 活跃告警`;
    active.classList.toggle("has", stats.active_alerts > 0);

    $("foot-mqtt").textContent = stats.mqtt
      ? `MQTT：${stats.mqtt.connected ? "已连接" : "未连接"} ${stats.mqtt.host}:${stats.mqtt.port} · ${stats.mqtt.messages} 条`
      : "MQTT：未启用（HTTP 上报）";

    // 拉曲线（只拉有数据的设备/指标）
    const jobs = [];
    for (const dev of devices) {
      for (const metric of dev.metrics) {
        jobs.push(
          getJSON(`/api/series?device=${encodeURIComponent(dev.device_id)}&metric=${encodeURIComponent(metric)}&window=${WINDOW_S}&bucket=${BUCKET_S}`)
            .then((data) => state.series.set(`${dev.device_id}|${metric}`, data.points))
            .catch(() => state.series.set(`${dev.device_id}|${metric}`, [])),
        );
      }
    }
    await Promise.all(jobs);

    const box = $("devices");
    box.innerHTML = "";
    devices.forEach((dev) => box.appendChild(deviceCard(dev)));
    box.querySelectorAll("canvas").forEach((c) => {
      const key = `${c.dataset.device}|${c.dataset.metric}`;
      drawChart(c, state.series.get(key) || []);
    });

    const tbody = $("alerts");
    tbody.innerHTML = "";
    alerts.forEach((a) => {
      const tr = document.createElement("tr");
      const resolved = a.resolved_ts !== null;
      tr.innerHTML = `
        <td class="num">${fmtTime(a.started_ts)}</td>
        <td>${a.device_id}</td>
        <td><span class="tag ${a.severity}">${a.rule_id}</span></td>
        <td class="num">${a.value.toFixed(2)}</td>
        <td class="num">${a.threshold}</td>
        <td><span class="tag ${resolved ? "resolved" : "active"}">${resolved ? "已恢复" : "告警中"}</span></td>`;
      tbody.appendChild(tr);
    });
    $("alerts-hint").classList.toggle("show", alerts.length === 0);

    const rulesBox = $("rules");
    rulesBox.innerHTML = "";
    rules.forEach((r) => {
      const li = document.createElement("li");
      li.innerHTML = `<code>${r.metric} ${r.op} ${r.threshold}</code>
        <span>持续 ${r.duration_s}s · 冷却 ${r.cooldown_s}s · ${r.severity}${r.device !== "*" ? ` · 仅 ${r.device}` : ""}</span>`;
      rulesBox.appendChild(li);
    });
  } catch (err) {
    state.errors += 1;
    $("live-dot").classList.remove("on");
    $("foot-mqtt").textContent = `刷新失败（连续 ${state.errors} 次）：${err.message}`;
  }
}

refresh();
setInterval(refresh, REFRESH_MS);
window.addEventListener("resize", () => {
  document.querySelectorAll("canvas").forEach((c) => {
    const key = `${c.dataset.device}|${c.dataset.metric}`;
    drawChart(c, state.series.get(key) || []);
  });
});
