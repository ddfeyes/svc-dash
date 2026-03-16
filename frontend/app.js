'use strict';

// ── Config ────────────────────────────────────────────────────────────────────
// Use same host for API (nginx will proxy /api to backend:8765)
const API = window.location.protocol + '//' + window.location.host + '/api';
const WS  = (window.location.protocol === 'https:' ? 'wss://' : 'ws://') + window.location.host;

const REFRESH_MS   = 15000;  // poll interval (15s to avoid backend overload)
const TRADE_MAX    = 100;    // max rows in tape


const ALERT_MAX    = 50;     // max rows in alerts feed
const WHALE_USD    = 10000;  // highlight threshold

// ── WS Stats ──────────────────────────────────────────────────────────────────
async function renderWsStats() {
  const data = await apiFetch('/ws-stats');
  const el = document.getElementById('ws-rate');
  if (!el) return;
  if (!data) { el.textContent = '— msg/s'; return; }

  const rate = data.messages_per_sec ?? 0;
  const conns = data.connections ?? 0;

  let rateText;
  if (rate >= 1000) rateText = (rate / 1000).toFixed(1) + 'k msg/s';
  else if (rate > 0) rateText = rate.toFixed(1) + ' msg/s';
  else rateText = '0 msg/s';

  const col = rate >= 100 ? 'var(--green)' : rate >= 10 ? 'var(--yellow)' : rate > 0 ? 'var(--muted)' : 'var(--muted)';
  el.textContent = `${conns}cx · ${rateText}`;
  el.style.color = col;
  el.title = `WebSocket: ${conns} connection(s) · ${rateText} · uptime ${Math.round(data.uptime_sec || 0)}s · total ${data.total_messages ?? 0} msgs`;
}

// ── Theme ─────────────────────────────────────────────────────────────────────
const THEME_KEY = 'theme';

function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  const btn = document.getElementById('theme-toggle');
  if (btn) btn.textContent = theme === 'dark' ? '☀' : '☾';
}

function toggleTheme() {
  const current = localStorage.getItem(THEME_KEY) || 'dark';
  const next = current === 'dark' ? 'light' : 'dark';
  localStorage.setItem(THEME_KEY, next);
  applyTheme(next);
}

function initTheme() {
  const saved = localStorage.getItem(THEME_KEY) || 'dark';
  applyTheme(saved);
  const btn = document.getElementById('theme-toggle');
  if (btn) btn.addEventListener('click', toggleTheme);
}

// ── State ─────────────────────────────────────────────────────────────────────
let activeSymbol = null;
let allSymbols   = [];
let priceChart   = null;   // TradingView Lightweight Charts instance
let oiChart      = null;   // Chart.js
let cvdChart     = null;   // Chart.js
let fundingChart = null;   // Chart.js
let spreadChart        = null;   // Chart.js
let aggressorChart     = null;   // Chart.js
let volumeProfileChart = null;   // Chart.js
let regimeTimelineChart = null;  // Chart.js
let wsAlerts     = null;

let refreshTimer = null;
let _lastPrice   = null;   // most recent close price (for OI USDT calc)

// ── Helpers ───────────────────────────────────────────────────────────────────
async function apiFetch(path, timeoutMs = 8000) {
  const url = API + path;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const r = await fetch(url, { signal: controller.signal });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  } catch (e) {
    console.warn('[apiFetch timeout]', path, e.message);
    return null;
  } finally {
    clearTimeout(timeoutId);
  }
}

function setErr(contentId) {
  const el = document.getElementById(contentId);
  if (!el) return;
  const txt = el.textContent.trim();
  if (txt.startsWith('Loading') || txt === 'No data yet' || txt === 'No data available' || txt === 'No data' || txt === 'Unavailable' || txt === '') {
    el.innerHTML = '<span class="card-badge badge-red" style="display:inline-block">Error</span>';
  }
}

/** Format a price with auto decimal places (6dp for sub-penny assets). */
function fmtPrice(price) {
  if (price == null) return '—';
  const v = parseFloat(price);
  if (isNaN(v)) return '—';
  const abs = Math.abs(v);
  let decimals;
  if (abs >= 1000)      decimals = 2;
  else if (abs >= 1)    decimals = 4;
  else if (abs >= 0.01) decimals = 5;
  else                  decimals = 6;
  return v.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

/** Generic number formatter (no $ prefix). */
function fmt(n, decimals = 4) {
  if (n == null) return '—';
  const v = parseFloat(n);
  if (isNaN(v)) return '—';
  return v.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

/** Format a large number as $47.5M, $1.2K, etc. */
function fmtUSD(n) {
  if (n == null) return '—';
  const v = parseFloat(n);
  if (isNaN(v)) return '—';
  const abs = Math.abs(v);
  const sign = v < 0 ? '-' : '';
  if (abs >= 1e9) return sign + '$' + (abs / 1e9).toFixed(2) + 'B';
  if (abs >= 1e6) return sign + '$' + (abs / 1e6).toFixed(2) + 'M';
  if (abs >= 1e3) return sign + '$' + (abs / 1e3).toFixed(1) + 'K';
  return sign + '$' + abs.toFixed(2);
}

/** Compact number without $ (for axes, OI display). */
function fmtK(n) {
  if (n == null) return '—';
  const v = Math.abs(parseFloat(n));
  if (v >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (v >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (v >= 1e3) return (n / 1e3).toFixed(1) + 'k';
  return parseFloat(n).toFixed(2);
}

function fmtTime(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function pctColor(v) {
  return v > 0 ? 'var(--green)' : v < 0 ? 'var(--red)' : 'var(--muted)';
}

function fundingColor(rate) {
  if (rate > 0.0005) return 'var(--red)';
  if (rate < -0.0005) return 'var(--green)';
  return 'var(--muted)';
}

// ── Symbol Tabs ───────────────────────────────────────────────────────────────
async function loadSymbols() {
  const data = await apiFetch('/symbols');
  if (!data?.symbols?.length) return;
  allSymbols = data.symbols;
  if (!activeSymbol) activeSymbol = allSymbols[0];
  renderSymbolTabs();
  document.title = activeSymbol + ' · Dashboard';
}

function renderSymbolTabs() {
  const el = document.getElementById('symbol-tabs');
  el.innerHTML = allSymbols.map(s => {
    const active = s === activeSymbol ? ' active' : '';
    return `<button class="sym-tab${active}" data-sym="${s}">${s.replace('USDT','')}</button>`;
  }).join('');

  el.querySelectorAll('.sym-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      if (btn.dataset.sym === activeSymbol) return;
      activeSymbol = btn.dataset.sym;
      document.title = activeSymbol + ' · Dashboard';
      el.querySelectorAll('.sym-tab').forEach(b => b.classList.toggle('active', b.dataset.sym === activeSymbol));
      resetCharts();
      refresh();
    });
  });
}

// ── Chart helpers ─────────────────────────────────────────────────────────────
function _chartDefaults(color) {
  return {
    animation: false,
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      tooltip: {
        mode: 'index',
        intersect: false,
        backgroundColor: '#1c2030',
        titleColor: '#6b7280',
        bodyColor: '#e2e8f0',
        borderColor: 'rgba(255,255,255,0.08)',
        borderWidth: 1,
      },
    },
    scales: {
      x: {
        ticks: { color: '#6b7280', font: { size: 9 }, maxTicksLimit: 8, maxRotation: 0 },
        grid: { color: 'rgba(255,255,255,0.04)' },
      },
      y: {
        ticks: { color: '#6b7280', font: { size: 9 } },
        grid: { color: 'rgba(255,255,255,0.04)' },
      },
    },
  };
}

// ── Chart Initialisation ──────────────────────────────────────────────────────
function initPriceChart() {
  const container = document.getElementById('price-chart-container');
  if (!window.LightweightCharts) {
    container.innerHTML = '<div class="loading">Charting library not loaded</div>';
    return;
  }

  priceChart = LightweightCharts.createChart(container, {
    width: container.clientWidth,
    height: container.clientHeight,
    layout: {
      background: { type: 'solid', color: '#141720' },
      textColor: '#6b7280',
      fontSize: 10,
      fontFamily: "'JetBrains Mono', monospace",
    },
    grid: {
      vertLines: { color: 'rgba(255,255,255,0.04)' },
      horzLines: { color: 'rgba(255,255,255,0.04)' },
    },
    crosshair: { mode: 1 },
    rightPriceScale: { borderColor: 'rgba(255,255,255,0.08)' },
    timeScale: {
      borderColor: 'rgba(255,255,255,0.08)',
      timeVisible: true,
      secondsVisible: false,
    },
    handleScroll: true,
    handleScale: true,
  });

  priceChart._candleSeries = priceChart.addCandlestickSeries({
    upColor: '#00e082',
    downColor: '#ff4d4f',
    borderUpColor: '#00e082',
    borderDownColor: '#ff4d4f',
    wickUpColor: '#00e082',
    wickDownColor: "#ff4d4f",
    priceFormat: { type: "price", precision: 6, minMove: 0.000001 },
  });

  priceChart._volumeSeries = priceChart.addHistogramSeries({
    color: 'rgba(78,168,222,0.3)',
    priceFormat: { type: 'volume' },
    priceScaleId: 'vol',
  });
  priceChart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });

  // Resize observer
  const ro = new ResizeObserver(entries => {
    for (const e of entries) {
      priceChart.applyOptions({ width: e.contentRect.width });
    }
  });
  ro.observe(container);
}

function initOiChart() {
  const canvas = document.getElementById('oi-canvas');
  if (!canvas || !window.Chart) return;
  const opts = _chartDefaults('#4ea8de');
  opts.plugins.tooltip.callbacks = { label: ctx => ' OI: ' + fmtUSD(ctx.raw) };
  opts.scales.y.ticks.callback = v => fmtUSD(v);
  oiChart = new Chart(canvas, {
    type: 'line',
    data: { labels: [], datasets: [{
      label: 'OI (USDT)',
      data: [],
      borderColor: '#4ea8de',
      backgroundColor: 'rgba(78,168,222,0.08)',
      borderWidth: 1.5,
      pointRadius: 0,
      tension: 0.3,
      fill: true,
    }]},
    options: opts,
  });
}

function initCvdChart() {
  const canvas = document.getElementById('cvd-canvas');
  if (!canvas || !window.Chart) return;
  const opts = _chartDefaults('#ab7df8');
  opts.plugins.tooltip.callbacks = { label: ctx => ' CVD: ' + fmtK(ctx.raw) };
  opts.scales.y.ticks.callback = v => fmtK(v);
  cvdChart = new Chart(canvas, {
    type: 'line',
    data: { labels: [], datasets: [{
      label: 'CVD',
      data: [],
      borderColor: '#ab7df8',
      backgroundColor: 'rgba(171,125,248,0.08)',
      borderWidth: 1.5,
      pointRadius: 0,
      tension: 0.2,
      fill: true,
    }]},
    options: opts,
  });
}

function initFundingChart() {
  const canvas = document.getElementById('funding-canvas');
  if (!canvas || !window.Chart) return;
  const opts = _chartDefaults('#f0c040');
  opts.plugins.tooltip.callbacks = { label: ctx => ' Rate: ' + (ctx.raw * 100).toFixed(4) + '%' };
  opts.scales.y.ticks.callback = v => (v * 100).toFixed(4) + '%';
  fundingChart = new Chart(canvas, {
    type: 'bar',
    data: { labels: [], datasets: [{
      label: 'Funding Rate',
      data: [],
      backgroundColor: [],
      borderWidth: 0,
    }]},
    options: opts,
  });
}

function initSpreadChart() {
  const canvas = document.getElementById('spread-canvas');
  if (!canvas || !window.Chart) return;
  const opts = _chartDefaults('#00e082');
  opts.plugins.tooltip.callbacks = { label: ctx => ' Spread: ' + ctx.raw.toFixed(4) + '%' };
  opts.scales.y.ticks.callback = v => v.toFixed(4) + '%';
  spreadChart = new Chart(canvas, {
    type: 'line',
    data: { labels: [], datasets: [{
      label: 'Spread %',
      data: [],
      borderColor: '#00e082',
      backgroundColor: 'rgba(0,224,130,0.06)',
      borderWidth: 1.5,
      pointRadius: 0,
      tension: 0.3,
      fill: true,
    }]},
    options: opts,
  });
}

function initAggressorChart() {
  const canvas = document.getElementById('aggressor-ratio-canvas');
  if (!canvas || !window.Chart) return;
  const opts = _chartDefaults('#00e082');
  opts.scales.x.stacked = true;
  opts.scales.y = {
    stacked: true,
    min: 0, max: 100,
    ticks: { color: '#6b7280', font: { size: 9 }, callback: v => v + '%' },
    grid: { color: 'rgba(255,255,255,0.04)' },
  };
  opts.plugins.tooltip.callbacks = {
    label: ctx => ` ${ctx.dataset.label}: ${ctx.raw.toFixed(1)}%`,
  };
aggressorChart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels: [],
      datasets: [
{ label: 'Buy',  data: [], backgroundColor: [], borderWidth: 0, stack: 'vp' },
        { label: 'Sell', data: [], backgroundColor: [], borderWidth: 0, stack: 'vp' },
      ],
    },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: 'y',
      plugins: {
        legend: { display: false },
        tooltip: {
          mode: 'index',
          intersect: false,
          backgroundColor: '#1c2030',
          titleColor: '#6b7280',
          bodyColor: '#e2e8f0',
          borderColor: 'rgba(255,255,255,0.08)',
          borderWidth: 1,
          callbacks: {
            title: ctx => `Price: ${ctx[0]?.label ?? ''}`,
            label: ctx => ` ${ctx.dataset.label}: ${fmtK(ctx.raw)}`,
          },
        },
      },
      scales: {
        x: {
          stacked: true,
          ticks: { color: '#6b7280', font: { size: 9 }, callback: v => fmtK(v) },
          grid: { color: 'rgba(255,255,255,0.04)' },
        },
        y: {
          ticks: { color: '#6b7280', font: { size: 8 }, maxTicksLimit: 14 },
          grid: { color: 'rgba(255,255,255,0.04)' },
        },
      },
    },
  });
}

// ── Render: Adaptive Volume Profile ──────────────────────────────────────────
async function renderAdaptiveVolumeProfile() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/volume-profile/adaptive?symbol=${sym}&bins=40`);
  const metricsEl = document.getElementById('adaptive-vp-metrics');
  const badge     = document.getElementById('adaptive-vp-badge');

  if (!data?.bins?.length) {
    if (metricsEl) metricsEl.innerHTML =
      '<div class="text-muted" style="font-size:11px;">Collecting session data…</div>';
    if (badge) badge.style.display = 'none';
    return;
  }

  // ── Metrics row ────────────────────────────────────────────────────────────
  if (metricsEl) {
    const sessionMins = Math.round((data.window_seconds || 0) / 60);
    metricsEl.innerHTML = `
      <span style="color:var(--muted)">POC <span style="color:var(--yellow);font-weight:700">${fmtPrice(data.poc)}</span></span>
      <span style="color:var(--muted)">VAH <span style="color:var(--green)">${fmtPrice(data.vah)}</span></span>
      <span style="color:var(--muted)">VAL <span style="color:var(--red)">${fmtPrice(data.val)}</span></span>
      <span style="color:var(--muted)">Vol <span style="color:var(--fg)">${fmtK(data.total_volume)}</span></span>
      <span style="color:var(--muted)">VA <span style="color:var(--yellow)">${(data.value_area_pct || 70).toFixed(1)}%</span></span>
      <span style="color:var(--muted)">Session <span style="color:var(--muted)">${sessionMins}m</span></span>
    `;
  }

  // ── Badge (POC price) ──────────────────────────────────────────────────────
  if (badge) {
    badge.textContent = 'POC ' + fmtPrice(data.poc);
    badge.style.display = 'inline-block';
  }

  if (!adaptiveVpChart) return;

  // Sort bins low→high (horizontal bar: bottom=low, top=high)
  const bins = [...data.bins].sort((a, b) => a.price - b.price);

  const labels   = bins.map(b => fmtPrice(b.price));
  const buyVols  = bins.map(b => b.buy_vol  || 0);
  const sellVols = bins.map(b => b.sell_vol || 0);

  // POC highlighted in yellow; value area semi-opaque; outside area faded
  const buyColors = bins.map(b =>
    b.is_poc        ? 'rgba(240,192,64,0.95)'
    : b.in_value_area ? 'rgba(0,224,130,0.55)'
    : 'rgba(0,224,130,0.20)'
  );
  const sellColors = bins.map(b =>
    b.is_poc        ? 'rgba(240,192,64,0.75)'
    : b.in_value_area ? 'rgba(255,77,79,0.55)'
    : 'rgba(255,77,79,0.20)'
  );

  adaptiveVpChart.data.labels                          = labels;
  adaptiveVpChart.data.datasets[0].data                = buyVols;
  adaptiveVpChart.data.datasets[0].backgroundColor     = buyColors;
  adaptiveVpChart.data.datasets[1].data                = sellVols;
  adaptiveVpChart.data.datasets[1].backgroundColor     = sellColors;

  adaptiveVpChart.update('none');
}


function resetCharts() {
  _lastTradeId = null;
  _lastPrice = null;

  if (priceChart?._candleSeries) {
    priceChart._candleSeries.setData([]);
    priceChart._volumeSeries.setData([]);
  }

  const clearChart = (ch) => {
    if (!ch) return;
    ch.data.labels = [];
    ch.data.datasets.forEach(ds => { ds.data = []; if (ds.backgroundColor instanceof Array) ds.backgroundColor = []; });
    ch.update('none');
  };

  clearChart(oiChart);
  clearChart(cvdChart);
  clearChart(fundingChart);
  clearChart(spreadChart);
clearChart(aggressorChart);
  clearChart(volumeProfileChart);
  clearChart(regimeTimelineChart);

  clearChart(aggressorChart);
  clearChart(volumeProfileChart);
  clearChart(regimeTimelineChart);

  document.getElementById('trade-tape').innerHTML = '';
  document.getElementById('cvd-metrics').innerHTML = '';
  document.getElementById('funding-metrics').innerHTML = '';
  document.getElementById('spread-metrics').innerHTML = '';
  document.getElementById('vol-imbalance-content').innerHTML = '';
  document.getElementById('oi-metrics').innerHTML = '';
const vpMetrics = document.getElementById('volume-profile-metrics');
  if (vpMetrics) vpMetrics.innerHTML = '';
  const arMetrics = document.getElementById('aggressor-ratio-metrics');
  if (arMetrics) arMetrics.innerHTML = '';
}


// ── Render: Price Chart (OHLCV) ───────────────────────────────────────────────
async function renderPriceChart() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/ohlcv?interval=60&window=7200&symbol=${sym}`);
  if (!data?.data?.length || !priceChart?._candleSeries) return;

  const candles = data.data
    .filter(c => c.open && c.close && c.high && c.low)
    .map(c => ({
      time:  Math.floor(c.ts),
      open:  parseFloat(c.open),
      high:  parseFloat(c.high),
      low:   parseFloat(c.low),
      close: parseFloat(c.close),
    }));

  const volumes = data.data
    .filter(c => c.ts && c.volume)
    .map(c => ({
      time:  Math.floor(c.ts),
      value: parseFloat(c.volume),
      color: parseFloat(c.close) >= parseFloat(c.open)
        ? 'rgba(0,224,130,0.3)'
        : 'rgba(255,77,79,0.3)',
    }));

  if (candles.length) {
    // Set Y-axis precision based on actual price magnitude
    const minPrice = Math.min(...candles.map(c => c.low));
    let precision, minMove;
    if (minPrice < 0.0001)      { precision = 8; minMove = 0.00000001; }
    else if (minPrice < 0.001)  { precision = 7; minMove = 0.0000001; }
    else if (minPrice < 0.01)   { precision = 6; minMove = 0.000001; }
    else if (minPrice < 0.1)    { precision = 5; minMove = 0.00001; }
    else if (minPrice < 1)      { precision = 4; minMove = 0.0001; }
    else if (minPrice < 10)     { precision = 3; minMove = 0.001; }
    else if (minPrice < 1000)   { precision = 2; minMove = 0.01; }
    else                        { precision = 2; minMove = 1; }
    priceChart._candleSeries.applyOptions({ priceFormat: { type: 'price', precision, minMove } });

    priceChart._candleSeries.setData(candles);
    priceChart._volumeSeries.setData(volumes);
    document.getElementById('price-chart-container').classList.remove('empty');

    const last = candles[candles.length - 1];
    const prev = candles.length > 1 ? candles[candles.length - 2] : null;
    updateLastPrice(last.close, prev ? last.close - prev.close : 0);
  }
}

function updateLastPrice(price, change) {
  _lastPrice = price;  // store for OI USDT calculation
  const priceEl  = document.getElementById('last-price');
  const changeEl = document.getElementById('price-change');
  if (priceEl) priceEl.textContent = fmtPrice(price);
  if (changeEl) {
    changeEl.textContent = (change >= 0 ? '+' : '') + fmtPrice(change);
    changeEl.className = change >= 0 ? 'up' : 'down';
  }
}

// ── Render: OI Chart (USDT) ───────────────────────────────────────────────────
async function renderOiChart() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/oi/history?limit=200&symbol=${sym}`);
  if (!data?.data?.length || !oiChart) return;

  const rows = data.data.filter(d => d.exchange === 'binance');
  const src = rows.length ? rows : data.data;

  const labels = src.map(d => fmtTime(d.ts));
  // Multiply raw OI (contracts/tokens) by current price to get USDT value.
  // Fetch price from trades if _lastPrice isn't set yet (parallel init).
  let price = _lastPrice;
  if (!price) {
    const td = await apiFetch(`/trades/recent?limit=1&symbol=${sym}`);
    if (td?.data?.length) {
      price = parseFloat(td.data[0].price);
      _lastPrice = price;
    }
    price = price || 1;
  }
  const values = src.map(d => parseFloat(d.oi_value) * price);

  oiChart.data.labels = labels;
  oiChart.data.datasets[0].data = values;
  oiChart.update('none');

  const first = values[0] || 0;
  const last  = values[values.length - 1] || 0;
  const pct   = first ? ((last - first) / first * 100) : 0;
  const pctStr = (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%';

  const el = document.getElementById('oi-metrics');
  if (el) {
    el.innerHTML = `
      <span class="oi-stat">OI <span style="color:${pctColor(pct)}">${fmtUSD(last)}</span></span>
      <span class="oi-stat">Δ <span style="color:${pctColor(pct)}">${pctStr}</span></span>
      <span class="oi-stat">exchange <span>${src[0]?.exchange || '—'}</span></span>
    `;
  }
}

// ── Render: CVD Chart ─────────────────────────────────────────────────────────
async function renderCvdChart() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/cvd/history?symbol=${sym}&window=3600`);
  if (!data?.data?.length || !cvdChart) return;

  const src = data.data;
  const labels = src.map(d => fmtTime(d.ts));
  const values = src.map(d => parseFloat(d.cvd));

  cvdChart.data.labels = labels;
  cvdChart.data.datasets[0].data = values;
  cvdChart.update('none');

  const lastCvd = values[values.length - 1] || 0;
  const firstCvd = values[0] || 0;
  const deltaCvd = lastCvd - firstCvd;
  const color = pctColor(lastCvd);

  const el = document.getElementById('cvd-metrics');
  if (el) {
    el.innerHTML = `
      <span class="cvd-stat">Current <span style="color:${color}">${fmtK(lastCvd)}</span></span>
      <span class="cvd-stat">1h Δ <span style="color:${pctColor(deltaCvd)}">${fmtK(deltaCvd)}</span></span>
      <span class="cvd-stat">Points <span>${src.length}</span></span>
    `;
  }
}

// ── Render: Funding Rate ──────────────────────────────────────────────────────
async function renderFunding() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/funding/history?symbol=${sym}&limit=100`);
  if (!data?.data?.length || !fundingChart) return;

  const src = data.data.slice().sort((a, b) => a.ts - b.ts);
  const labels = src.map(d => fmtTime(d.ts));
  const values = src.map(d => parseFloat(d.rate));
  const colors = values.map(v => v >= 0 ? 'rgba(255,77,79,0.7)' : 'rgba(0,224,130,0.7)');

  fundingChart.data.labels = labels;
  fundingChart.data.datasets[0].data = values;
  fundingChart.data.datasets[0].backgroundColor = colors;
  fundingChart.update('none');

  const last = values[values.length - 1];
  const avg  = values.reduce((a, b) => a + b, 0) / values.length;
  const pct  = last != null ? (last * 100).toFixed(4) + '%' : '—';
  const avgPct = avg != null ? (avg * 100).toFixed(4) + '%' : '—';
  const col  = fundingColor(last);

  // Badge
  const badge = document.getElementById('funding-badge');
  if (badge && last != null) {
    if (Math.abs(last) > 0.001) {
      badge.textContent = last > 0 ? 'longs pay' : 'shorts pay';
      badge.className = 'card-badge ' + (last > 0 ? 'badge-red' : 'badge-green');
      badge.style.display = 'inline-block';
    } else {
      badge.style.display = 'none';
    }
  }

  const el = document.getElementById('funding-metrics');
  if (el) {
    el.innerHTML = `
      <span class="funding-stat">Current <span style="color:${col}">${pct}</span></span>
      <span class="funding-stat">Avg <span style="color:${pctColor(avg)}">${avgPct}</span></span>
      <span class="funding-stat">Exchange <span>${src[0]?.exchange || '—'}</span></span>
    `;
  }
}

// ── Render: Funding Momentum ──────────────────────────────────────────────────
async function renderFundingMomentum() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/funding-momentum?symbol=${sym}&periods=4`);
  if (!data || data.status !== 'ok') return;

  const { current_rate, momentum, momentum_pct, trend } = data;

  // Trend arrow
  const arrows = { accelerating: '↑', decelerating: '↓', stable: '→' };
  const arrow = arrows[trend] || '→';

  const fmtRate = v => v != null ? (v * 100).toFixed(4) + '%' : '—';
  const fmtMom  = v => v != null ? (v >= 0 ? '+' : '') + (v * 100).toFixed(4) + '%' : '—';
  const momColor = momentum != null && momentum > 0 ? 'var(--green)' : momentum < 0 ? 'var(--red)' : 'var(--muted)';
  const trendColor = trend === 'accelerating' ? 'var(--green)' : trend === 'decelerating' ? 'var(--red)' : 'var(--muted)';

  // Badge
  const badge = document.getElementById('funding-momentum-badge');
  if (badge && momentum != null) {
    if (Math.abs(momentum) > 1e-5) {
      badge.textContent = arrow + ' ' + trend;
      badge.className = 'card-badge ' + (momentum > 0 ? 'badge-red' : 'badge-green');
      badge.style.display = 'inline-block';
    } else {
      badge.style.display = 'none';
    }
  }

  const el = document.getElementById('funding-momentum-metrics');
  if (el) {
    el.innerHTML = `
      <span class="funding-stat">Current <span style="color:${fundingColor(current_rate)}">${fmtRate(current_rate)}</span></span>
      <span class="funding-stat">Momentum <span style="color:${momColor}">${fmtMom(momentum)}</span></span>
      <span class="funding-stat">Chg% <span style="color:${momColor}">${momentum_pct != null ? (momentum_pct >= 0 ? '+' : '') + momentum_pct.toFixed(2) + '%' : '—'}</span></span>
      <span class="funding-stat">Trend <span style="color:${trendColor}">${arrow} ${trend}</span></span>
    `;
  }
}

// ── Render: Bid-Ask Spread ────────────────────────────────────────────────────
async function renderSpread() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/spread-history?symbol=${sym}&window=1800`);
  if (!data?.data?.length || !spreadChart) return;

  const src = data.data;
  // Downsample to ~200 points for performance
  const step = Math.max(1, Math.floor(src.length / 200));
  const pts = src.filter((_, i) => i % step === 0);

  const labels = pts.map(d => fmtTime(d.ts));
  const values = pts.map(d => parseFloat(d.spread_pct));

  spreadChart.data.labels = labels;
  spreadChart.data.datasets[0].data = values;
  spreadChart.update('none');

  const last = values[values.length - 1] || 0;
  const avg  = values.reduce((a, b) => a + b, 0) / values.length;

  // Alert badge
  const badge = document.getElementById('spread-badge');
  if (badge && data.alert) {
    badge.textContent = 'wide';
    badge.className = 'card-badge badge-red';
    badge.style.display = 'inline-block';
  } else if (badge) {
    badge.style.display = 'none';
  }

  const el = document.getElementById('spread-metrics');
  if (el) {
    const lastBps = data.data[data.data.length - 1]?.spread_bps || 0;
    el.innerHTML = `
      <span class="spread-stat">Current <span>${last.toFixed(4)}% (${lastBps.toFixed(1)} bps)</span></span>
      <span class="spread-stat">Avg <span>${avg.toFixed(4)}%</span></span>
    `;
  }
}

// ── Render: Volume Imbalance ──────────────────────────────────────────────────
async function renderVolumeImbalance() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/volume-imbalance?symbol=${sym}&window=60`);
  if (!data) return;

  const buyVol  = data.buy_volume  || 0;
  const sellVol = data.sell_volume || 0;
  const total   = data.total_volume || (buyVol + sellVol) || 1;
  const imb     = data.imbalance != null ? data.imbalance : (buyVol - sellVol) / total;

  const buyPct  = ((buyVol / total) * 100).toFixed(1);
  const sellPct = ((sellVol / total) * 100).toFixed(1);
  const imbColor = imb > 0 ? 'var(--green)' : imb < 0 ? 'var(--red)' : 'var(--muted)';
  const imbLabel = imb > 0 ? 'Buy pressure' : imb < 0 ? 'Sell pressure' : 'Balanced';

  const el = document.getElementById('vol-imbalance-content');
  if (!el) return;

  el.innerHTML = `
    <div class="imbalance-bar-wrap">
      <div class="imbalance-labels">
        <span style="color:var(--green)">BUY ${buyPct}%</span>
        <span style="color:var(--red)">SELL ${sellPct}%</span>
      </div>
      <div class="imbalance-track">
        <div class="imbalance-fill-buy"  style="width:${buyPct}%"></div>
        <div class="imbalance-fill-sell" style="width:${sellPct}%"></div>
      </div>
      <div class="imbalance-nums">
        <span>${fmtK(buyVol)}</span>
        <span>${fmtK(sellVol)}</span>
      </div>
    </div>
    <div class="imbalance-score" style="color:${imbColor}">
      ${imbLabel} (${(imb * 100).toFixed(1)}%)
    </div>
    <div style="font-size:9px;color:var(--muted);text-align:center">
      Total: ${fmtK(total)} · window: 60s
    </div>
  `;
}

// ── Render: Trade Tape ────────────────────────────────────────────────────────
let _lastTradeId = null;

async function renderTradeTape() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/trades/recent?limit=50&symbol=${sym}`);
  if (!data?.data?.length) return;

  const tape = document.getElementById('trade-tape');
  const trades = data.data;

  const lastId = _lastTradeId;
  const newTrades = lastId
    ? trades.filter(t => t.id > lastId)
    : trades;

  if (!newTrades.length) return;
  _lastTradeId = Math.max(...trades.map(t => t.id || 0));

  const rows = newTrades.map(t => {
    const side  = (t.side || '').toLowerCase();
    const price = parseFloat(t.price);
    const qty   = parseFloat(t.qty);
    const usd   = price * qty;
    const whale = usd >= WHALE_USD ? ' whale' : '';
    return `<div class="trade-row${whale}">
        <span class="trade-side ${side}">${side === 'buy' ? 'B' : 'S'}</span>
        <span class="trade-price">${fmtPrice(price)}</span>
        <span class="trade-qty">${fmtK(qty)}</span>
        <span class="trade-time">${fmtTime(t.ts)}</span>
      </div>`;
  }).join('');

  tape.insertAdjacentHTML('afterbegin', rows);

  while (tape.children.length > TRADE_MAX) {
    tape.removeChild(tape.children[tape.children.length - 1]);
  }
}

// ── Render: Phase / Market Regime ─────────────────────────────────────────────
async function renderPhase() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/market-regime?symbol=${sym}`);
  if (!data) {
    setErr('phase-content');
    return;
  }

  const phase  = data.phase  || data.regime || 'Unknown';
  const regime = data.regime || phase;
  const conf   = data.phase_confidence != null ? (data.phase_confidence * 100).toFixed(0) + '%' : '—';
  const score  = data.score  != null ? data.score.toFixed(1) : '—';
  const action = data.action || '—';

  const phaseColors = {
    accumulation: 'var(--green)',
    distribution: 'var(--red)',
    markup:       'var(--blue)',
    markdown:     'var(--red)',
    ranging:      'var(--yellow)',
    bear:         'var(--red)',
    bull:         'var(--green)',
  };
  const color = phaseColors[(phase || '').toLowerCase()] || 'var(--fg)';
  const scoreColor = data.score > 0 ? 'var(--green)' : data.score < 0 ? 'var(--red)' : 'var(--muted)';

  const el = document.getElementById('phase-content');
  if (!el) return;

  el.innerHTML = `
    <div class="phase-name" style="color:${color}">${phase}</div>
    <div class="phase-metrics">
      <div class="metric-box">
        <div class="metric-label">Regime</div>
        <div class="metric-value" style="color:${color};font-size:12px;">${regime}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Confidence</div>
        <div class="metric-value" style="color:${color}">${conf}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Score</div>
        <div class="metric-value" style="color:${scoreColor}">${score}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Action</div>
        <div class="metric-value" style="color:var(--muted);font-size:11px;">${action}</div>
      </div>
    </div>
  `;

  const badge = document.getElementById('phase-badge');
  if (badge) {
    badge.textContent = phase;
    badge.className = 'card-badge ' + (
      ['accumulation','markup','bull'].includes((phase||'').toLowerCase()) ? 'badge-green' :
      ['distribution','markdown','bear'].includes((phase||'').toLowerCase()) ? 'badge-red' :
      ['ranging'].includes((phase||'').toLowerCase()) ? 'badge-yellow' :
      'badge-blue'
    );
    badge.style.display = 'inline-block';
  }
}

// ── Render: Inter-Exchange OI Divergence ─────────────────────────────────────
async function renderOiDivergence() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/oi-divergence?symbol=${sym}&window=3600`);
  if (!data) {
    setErr('oi-divergence-content');
    return;
  }

  const badge = document.getElementById('oi-divergence-badge');
  if (badge) {
    if (data.divergence) {
      const sevClass = data.severity === 'high'   ? 'badge-red'
                     : data.severity === 'medium' ? 'badge-yellow'
                     : 'badge-blue';
      badge.textContent = data.severity.toUpperCase()
        + (data.opposing ? ' · OPPOSING' : '');
      badge.className = `card-badge ${sevClass}`;
      badge.style.display = 'inline-block';
    } else {
      badge.style.display = 'none';
    }
  }

  const el = document.getElementById('oi-divergence-content');
  if (!el) return;

  const exs = data.exchanges || {};
  const exNames = Object.keys(exs);

  if (!exNames.length) {
    el.innerHTML = '<div class="text-muted" style="font-size:11px;">No exchange data yet</div>';
    return;
  }

  const divColor = data.divergence
    ? (data.severity === 'high' ? 'var(--red)' : data.severity === 'medium' ? 'var(--yellow)' : '#64b4ff')
    : 'var(--green)';

  const rows = exNames.map(ex => {
    const e = exs[ex];
    const pctStr = e.pct_change >= 0 ? `+${e.pct_change.toFixed(2)}%` : `${e.pct_change.toFixed(2)}%`;
    const devStr = e.deviation  >= 0 ? `+${e.deviation.toFixed(2)}%`  : `${e.deviation.toFixed(2)}%`;
    const dirColor = e.direction === 'up' ? 'var(--green)' : e.direction === 'down' ? 'var(--red)' : 'var(--muted)';
    const isDiverging = data.diverging_exchange === ex;
    return `
      <div class="metric-box" style="${isDiverging ? 'border:1px solid ' + divColor + ';border-radius:6px;' : ''}">
        <div class="metric-label">${ex.charAt(0).toUpperCase() + ex.slice(1)}</div>
        <div class="metric-value" style="color:${dirColor}">${pctStr}</div>
        <div class="metric-label">dev ${devStr}</div>
      </div>`;
  }).join('');

  const meanStr = data.mean_pct_change >= 0
    ? `+${data.mean_pct_change.toFixed(2)}%`
    : `${data.mean_pct_change.toFixed(2)}%`;

  el.innerHTML = `
    <div class="phase-metrics">
      ${rows}
      <div class="metric-box">
        <div class="metric-label">Mean</div>
        <div class="metric-value" style="color:var(--muted)">${meanStr}</div>
        <div class="metric-label">max dev ${data.divergence_pct.toFixed(2)}%</div>
      </div>
    </div>
    <div style="font-size:10px;color:var(--muted);margin-top:6px;line-height:1.4">${data.description || ''}</div>
  `;
}

// ── Render: Microstructure Score ──────────────────────────────────────────────
function _compColor(score) {
  if (score == null) return 'var(--muted)';
  return score >= 80 ? 'var(--green)'
       : score >= 60 ? '#64b4ff'
       : score >= 40 ? 'var(--yellow)'
       : 'var(--red)';
}

async function renderMicrostructure() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/market-microstructure?symbol=${sym}&window=300`);
  if (!data) {
    setErr('microstructure-content');
    return;
  }

  const badge = document.getElementById('microstructure-badge');
  if (badge) {
    badge.textContent = data.grade + ' · ' + (data.label || '');
    badge.className = 'card-badge '
      + (data.grade === 'A' ? 'badge-green'
       : data.grade === 'B' ? 'badge-blue'
       : data.grade === 'C' ? 'badge-yellow'
       : 'badge-red');
    badge.style.display = 'inline-block';
  }

  const el = document.getElementById('microstructure-content');
  if (!el) return;

  const c   = data.components || {};
  const scoreColor = _compColor(data.score);

  const spreadVal    = c.spread?.value     != null ? c.spread.value.toFixed(1) + ' bps' : '—';
  const depthVal     = c.depth?.value      != null ? '$' + fmtK(c.depth.value)          : '—';
  const rateVal      = c.trade_rate?.value != null ? c.trade_rate.value.toFixed(2) + '/s' : '—';
  const noiseVal     = c.noise?.value      != null ? (c.noise.value * 100).toFixed(2) + '%' : '—';

  el.innerHTML = `
    <div class="phase-metrics">
      <div class="metric-box">
        <div class="metric-label">Score</div>
        <div class="metric-value" style="color:${scoreColor};font-size:22px">${data.score}</div>
        <div class="metric-label" style="color:${scoreColor}">${data.label || ''}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Spread</div>
        <div class="metric-value" style="color:${_compColor(c.spread?.score)}">${c.spread?.score ?? '—'}</div>
        <div class="metric-label">${spreadVal}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Depth</div>
        <div class="metric-value" style="color:${_compColor(c.depth?.score)}">${c.depth?.score ?? '—'}</div>
        <div class="metric-label">${depthVal}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Trade Rate</div>
        <div class="metric-value" style="color:${_compColor(c.trade_rate?.score)}">${c.trade_rate?.score ?? '—'}</div>
        <div class="metric-label">${rateVal}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Noise</div>
        <div class="metric-value" style="color:${_compColor(c.noise?.score)}">${c.noise?.score ?? '—'}</div>
        <div class="metric-label">${noiseVal}</div>
      </div>
    </div>
  `;
}

// ── WebSocket: Alerts ─────────────────────────────────────────────────────────
function connectAlerts() {
  if (wsAlerts) {
    wsAlerts.close();
    wsAlerts = null;
  }

  const url = WS + '/api/ws/alerts';
  try {
    wsAlerts = new WebSocket(url);
  } catch (e) {
    console.warn('[WS] failed to create WebSocket:', e);
    return;
  }

  const statusEl = document.getElementById('header-status');

  wsAlerts.onopen = () => {
    if (statusEl) {
      statusEl.textContent = 'connected';
      statusEl.className = 'connected';
    }
  };

  wsAlerts.onmessage = (evt) => {
    try {
      const msg = JSON.parse(evt.data);
      if (msg.type === 'ping') return;
      const text = msg.description || msg.message || null;
      if (text) {
        showAlertBanner(text);
        appendAlertFeed(msg);
      }
    } catch (_) {}
  };

  wsAlerts.onclose = () => {
    if (statusEl) {
      statusEl.textContent = 'disconnected — reconnecting…';
      statusEl.className = 'disconnected';
    }
    setTimeout(connectAlerts, 5000);
  };

  wsAlerts.onerror = (e) => {
    console.warn('[WS] error', e);
  };
}

function showAlertBanner(text) {
  const bar = document.getElementById('alert-bar');
  if (!bar) return;
  bar.textContent = '⚡ ' + text;
  bar.classList.add('visible');
  clearTimeout(bar._timeout);
  bar._timeout = setTimeout(() => bar.classList.remove('visible'), 8000);
}

function appendAlertFeed(msg) {
  const feed = document.getElementById('alerts-feed');
  if (!feed) return;

  const ts   = msg.ts ? fmtTime(msg.ts) : fmtTime(Date.now() / 1000);
  const text = msg.description || msg.message || JSON.stringify(msg);
  const sev  = msg.severity || msg.level || 'low';
  const sevClass = sev === 'high' ? 'sev-high' : sev === 'medium' ? 'sev-medium' : 'sev-low';

  const row = document.createElement('div');
  row.className = `alert-row ${sevClass}`;
  row.innerHTML = `<span class="alert-time">${ts}</span><span class="alert-text">${text}</span>`;
  feed.insertBefore(row, feed.firstChild);

  while (feed.children.length > ALERT_MAX) {
    feed.removeChild(feed.lastChild);
  }
}

// ── Whale Order Clustering ────────────────────────────────────────────────────
async function renderWhaleClustering() {
  const el = document.getElementById('whale-clustering-content');
  const badge = document.getElementById('whale-clustering-badge');
  if (!el) return;

  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/whale-clustering?symbol=${sym}&window=1800`);
  if (!data) {
    setErr('whale-clustering-content');
    return;
  }

  if (!data || data.trade_count === 0) {
    el.innerHTML = '<div class="text-muted" style="font-size:11px;">No whale trades in window</div>';
    badge.style.display = 'none';
    return;
  }

  const zones = data.zones || [];
  const bins = data.bins || [];

  if (zones.length > 0) {
    badge.textContent = `${zones.length} zone${zones.length > 1 ? 's' : ''}`;
    badge.style.display = 'inline-block';
    badge.style.background = 'var(--accent, #f59e0b)';
  } else {
    badge.style.display = 'none';
  }

  const fmtK = v => v >= 1e6 ? `$${(v/1e6).toFixed(2)}M` : v >= 1e3 ? `$${(v/1e3).toFixed(1)}K` : `$${v.toFixed(0)}`;
  const topZone = data.top_zone_price != null ? `$${Number(data.top_zone_price).toLocaleString()}` : '—';

  let html = `<div style="display:flex;gap:16px;flex-wrap:wrap;font-size:11px;margin-bottom:8px;padding:0 2px;">
    <span>Trades <b>${data.trade_count}</b></span>
    <span>Volume <b>${fmtK(data.total_usd)}</b></span>
    <span>Bins <b>${data.non_empty_bins}</b></span>
    <span>Zones <b>${zones.length}</b></span>
    <span>Top Zone <b>${topZone}</b></span>
  </div>`;

  if (bins.length > 0) {
    const maxVol = Math.max(...bins.map(b => b.total_usd));
    html += '<div style="display:flex;flex-direction:column;gap:2px;font-size:10px;">';
    for (const b of [...bins].reverse()) {
      const pct = maxVol > 0 ? (b.total_usd / maxVol * 100).toFixed(1) : 0;
      const barColor = b.is_zone
        ? 'var(--accent, #f59e0b)'
        : b.dominance === 'buy' ? 'var(--bull, #22c55e)' : b.dominance === 'sell' ? 'var(--bear, #ef4444)' : 'var(--muted, #6b7280)';
      html += `<div style="display:flex;align-items:center;gap:6px;">
        <span style="width:56px;text-align:right;color:var(--muted);">$${Number(b.price_mid).toLocaleString()}</span>
        <div style="flex:1;background:var(--bg2,#1e1e2e);border-radius:2px;height:10px;position:relative;">
          <div style="width:${pct}%;height:100%;background:${barColor};border-radius:2px;opacity:${b.is_zone ? 1 : 0.55};"></div>
        </div>
        <span style="width:44px;color:var(--muted);">${fmtK(b.total_usd)}</span>
        ${b.is_zone ? '<span style="color:var(--accent,#f59e0b);font-weight:700;">ZONE</span>' : ''}
      </div>`;
    }
    html += '</div>';
  }

  el.innerHTML = html;
}

// ── Render: VWAP Deviation ────────────────────────────────────────────────────
async function renderVwapDeviation() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/vwap-deviation?symbol=${sym}`);
  if (!data) {
    setErr('vwap-deviation-content');
    return;
  }

  const el    = document.getElementById('vwap-deviation-content');
  const badge = document.getElementById('vwap-deviation-badge');
  if (!el) return;

  const devPct  = data.deviation_pct != null ? parseFloat(data.deviation_pct) : null;
  const devColor = devPct == null ? 'var(--muted)' : devPct > 0 ? 'var(--green)' : 'var(--red)';
  const devStr  = devPct != null ? (devPct > 0 ? '+' : '') + devPct.toFixed(3) + '%' : '—';
  const signal  = data.signal || '—';
  const vwap    = data.vwap          != null ? fmtPrice(data.vwap)          : '—';
  const price   = data.current_price != null ? fmtPrice(data.current_price) : '—';

  if (badge) {
    badge.textContent = signal;
    badge.className   = 'card-badge ' + (devPct > 0 ? 'badge-green' : devPct < 0 ? 'badge-red' : 'badge-blue');
    badge.style.display = 'inline-block';
  }

  el.innerHTML = `
    <div class="phase-metrics">
      <div class="metric-box">
        <div class="metric-label">Deviation</div>
        <div class="metric-value" style="color:${devColor};font-size:22px">${devStr}</div>
        <div class="metric-label" style="color:${devColor}">${signal}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Price</div>
        <div class="metric-value">${price}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">VWAP</div>
        <div class="metric-value" style="color:var(--muted)">${vwap}</div>
      </div>
    </div>
  `;
}

// ── Render: OI-Weighted Price ─────────────────────────────────────────────────
async function renderOiWeightedPrice() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/oi-weighted-price?symbol=${sym}`);
  const el    = document.getElementById('oi-weighted-price-content');
  const badge = document.getElementById('oi-weighted-price-badge');
  if (!el) return;

  if (!data) {
    setErr('oi-weighted-price-content');
    return;
  }

  const devPct   = data.deviation_pct != null ? parseFloat(data.deviation_pct) : null;
  const bias     = data.bias || 'neutral';
  const oiWp     = data.oi_weighted_price != null ? fmtPrice(data.oi_weighted_price) : '—';
  const curPrice = data.current_price     != null ? fmtPrice(data.current_price)     : '—';
  const devStr   = devPct != null ? (devPct >= 0 ? '+' : '') + devPct.toFixed(3) + '%' : '—';

  // Red = price above OI weight (overextended longs), green = below (overextended shorts)
  const devColor = devPct == null  ? 'var(--muted)'
                 : devPct >  1.0   ? 'var(--red)'
                 : devPct < -1.0   ? 'var(--green)'
                 :                   'var(--muted)';

  if (badge) {
    badge.textContent = bias.replace('_', ' ');
    badge.className   = 'card-badge ' + (bias === 'long_heavy' ? 'badge-red' : bias === 'short_heavy' ? 'badge-green' : 'badge-blue');
    badge.style.display = 'inline-block';
  }

  el.innerHTML = `
    <div class="phase-metrics">
      <div class="metric-box">
        <div class="metric-label">Deviation</div>
        <div class="metric-value" style="color:${devColor};font-size:22px">${devStr}</div>
        <div class="metric-label" style="color:${devColor}">${bias.replace('_', ' ')}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Price</div>
        <div class="metric-value">${curPrice}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">OI Anchor</div>
        <div class="metric-value" style="color:var(--muted)">${oiWp}</div>
      </div>
    </div>
  `;
}

// ── Render: Realized Vol Bands ────────────────────────────────────────────────
async function renderRealizedVolBands() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/realized-volatility-bands?symbol=${sym}&window=20`);
  const el    = document.getElementById('realized-vol-bands-content');
  const badge = document.getElementById('realized-vol-bands-badge');
  if (!el) return;

  if (!data) {
    setErr('realized-vol-bands-content');
    return;
  }

  const upper   = data.upper         != null ? fmtPrice(data.upper)         : '—';
  const center  = data.center        != null ? fmtPrice(data.center)        : '—';
  const lower   = data.lower         != null ? fmtPrice(data.lower)         : '—';
  const curP    = data.current_price != null ? fmtPrice(data.current_price) : '—';
  const vol     = data.realized_vol  != null ? (data.realized_vol * 100).toFixed(4) + '%' : '—';
  const pct     = data.band_percentile != null ? parseFloat(data.band_percentile) : null;
  const zone    = data.zone || 'inside';

  const zoneColor = zone === 'above_upper' ? 'var(--red)'
                  : zone === 'below_lower' ? 'var(--green)'
                  :                          'var(--yellow)';

  const pctStr = pct != null ? pct.toFixed(1) + '%' : '—';

  // Percentile bar fill
  const barFill = pct != null ? Math.round(pct) : 50;
  const barColor = zone === 'above_upper' ? '#e74c3c'
                 : zone === 'below_lower' ? '#2ecc71'
                 :                          '#f39c12';

  if (badge) {
    badge.textContent = zone.replace(/_/g, ' ');
    badge.className   = 'card-badge ' + (zone === 'above_upper' ? 'badge-red' : zone === 'below_lower' ? 'badge-green' : 'badge-yellow');
    badge.style.display = 'inline-block';
  }

  el.innerHTML = `
    <div class="phase-metrics">
      <div class="metric-box">
        <div class="metric-label">Percentile</div>
        <div class="metric-value" style="color:${zoneColor};font-size:22px">${pctStr}</div>
        <div class="metric-label" style="color:${zoneColor}">${zone.replace(/_/g, ' ')}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Upper</div>
        <div class="metric-value" style="color:var(--red);font-size:13px">${upper}</div>
        <div class="metric-label">Center</div>
        <div class="metric-value" style="font-size:13px">${center}</div>
        <div class="metric-label">Lower</div>
        <div class="metric-value" style="color:var(--green);font-size:13px">${lower}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Price</div>
        <div class="metric-value" style="font-size:13px">${curP}</div>
        <div class="metric-label">RealVol/c</div>
        <div class="metric-value" style="color:var(--muted);font-size:13px">${vol}</div>
      </div>
    </div>
    <div style="margin-top:6px;padding:0 4px">
      <div style="background:var(--bg2);border-radius:3px;height:6px;position:relative">
        <div style="position:absolute;left:0;top:0;height:6px;width:${barFill}%;background:${barColor};border-radius:3px;transition:width 0.4s"></div>
        <div style="position:absolute;left:50%;top:-2px;width:2px;height:10px;background:var(--muted);opacity:0.4"></div>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:9px;color:var(--muted);margin-top:2px">
        <span>lower</span><span>center</span><span>upper</span>
      </div>
    </div>
  `;
}

// ── Render: Market Regime ─────────────────────────────────────────────────────
async function renderMarketRegime() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/market-regime?symbol=${sym}`);
  if (!data) {
    setErr('market-regime-content');
    return;
  }

  const el    = document.getElementById('market-regime-content');
  const badge = document.getElementById('market-regime-badge');
  if (!el) return;

  const regime = data.regime || '—';
  const phase  = data.phase  || '—';
  const score  = data.score  != null ? parseFloat(data.score).toFixed(1) : '—';
  const conf   = data.phase_confidence != null ? (parseFloat(data.phase_confidence) * 100).toFixed(0) + '%' : '—';
  const action = data.action || '—';
  const weights = data.weights || {};

  const regimeColors = { Bull: 'var(--green)', Bear: 'var(--red)', Neutral: 'var(--yellow)' };
  const color = regimeColors[regime] || 'var(--fg)';
  const scoreColor = parseFloat(data.score) > 0 ? 'var(--green)' : parseFloat(data.score) < 0 ? 'var(--red)' : 'var(--muted)';

  if (badge) {
    badge.textContent = regime;
    badge.className   = 'card-badge ' + (
      regime === 'Bull' ? 'badge-green' :
      regime === 'Bear' ? 'badge-red' :
      'badge-yellow'
    );
    badge.style.display = 'inline-block';
  }

  const wParts = Object.entries(weights)
    .filter(([, v]) => v !== 0)
    .map(([k, v]) => {
      const vc = v > 0 ? 'var(--green)' : 'var(--red)';
      return `<span style="color:var(--muted)">${k} <span style="color:${vc}">${v > 0 ? '+' : ''}${v.toFixed(1)}</span></span>`;
    }).join(' · ');

  el.innerHTML = `
    <div class="phase-name" style="color:${color};font-size:16px">${regime}</div>
    <div class="phase-metrics">
      <div class="metric-box">
        <div class="metric-label">Phase</div>
        <div class="metric-value" style="color:${color};font-size:13px">${phase}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Score</div>
        <div class="metric-value" style="color:${scoreColor}">${score}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Confidence</div>
        <div class="metric-value" style="color:${color}">${conf}</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Action</div>
        <div class="metric-value" style="font-size:11px;color:var(--muted)">${action}</div>
      </div>
    </div>
    ${wParts ? `<div style="font-size:9px;color:var(--muted);margin-top:4px;line-height:1.6">${wParts}</div>` : ''}
  `;
}

// ── Render: Momentum ──────────────────────────────────────────────────────────
async function renderMomentum() {
  const data = await apiFetch('/momentum');
  const el = document.getElementById('momentum-content');
  if (!el) return;

  if (!data?.symbols || !Object.keys(data.symbols).length) {
    el.innerHTML = '<div class="text-muted" style="font-size:11px;">No data</div>';
    return;
  }

  function fmtPct(v) {
    if (v == null) return '<span style="color:var(--muted)">—</span>';
    const n = parseFloat(v);
    if (isNaN(n)) return '<span style="color:var(--muted)">—</span>';
    const sign = n > 0 ? '+' : '';
    const col  = n > 0 ? 'var(--green)' : n < 0 ? 'var(--red)' : 'var(--muted)';
    return `<span style="color:${col}">${sign}${n.toFixed(2)}%</span>`;
  }

  const rows = Object.entries(data.symbols).map(([sym, d]) => {
    return `<tr>
      <td style="color:var(--fg);padding:3px 6px;font-size:10px;">${sym.replace('USDT','')}</td>
      <td style="text-align:right;padding:3px 6px;font-size:10px;">${fmtPct(d['1h'])}</td>
      <td style="text-align:right;padding:3px 6px;font-size:10px;">${fmtPct(d['4h'])}</td>
      <td style="text-align:right;padding:3px 6px;font-size:10px;">${fmtPct(d['24h'])}</td>
    </tr>`;
  }).join('');

  el.innerHTML = `
    <table style="width:100%;border-collapse:collapse;">
      <thead>
        <tr>
          <th style="text-align:left;padding:3px 6px;font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;">Symbol</th>
          <th style="text-align:right;padding:3px 6px;font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;">1h</th>
          <th style="text-align:right;padding:3px 6px;font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;">4h</th>
          <th style="text-align:right;padding:3px 6px;font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;">24h</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

// ── Render: Regime Timeline ────────────────────────────────────────────────────
async function renderRegimeTimeline() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/phase-history?symbol=${sym}&limit=30`);
  if (!data?.data?.length || !regimeTimelineChart) return;

  const phaseScore = { Markup: 1, Accumulation: 0.5, Ranging: 0, Distribution: -0.5, Markdown: -1 };
  const phaseColor = {
    Markup:       'rgba(78,168,222,0.85)',
    Accumulation: 'rgba(0,224,130,0.85)',
    Ranging:      'rgba(240,192,64,0.75)',
    Distribution: 'rgba(255,140,50,0.85)',
    Markdown:     'rgba(255,77,79,0.85)',
  };

  const src = data.data.slice().sort((a, b) => a.ts - b.ts);
  regimeTimelineChart.data.labels = src.map(d => d.phase || '?');
  regimeTimelineChart.data.datasets[0].data = src.map(d => phaseScore[d.phase] ?? 0);
  regimeTimelineChart.data.datasets[0].backgroundColor = src.map(d => phaseColor[d.phase] || 'rgba(107,114,128,0.6)');
  regimeTimelineChart.update('none');
}

// ── Render: Correlations ──────────────────────────────────────────────────────
async function renderCorrelations() {
  const data = await apiFetch('/correlations');
  const el = document.getElementById('correlations-content');
  if (!el) return;

  if (!data?.matrix) {
    el.innerHTML = '<div class="text-muted" style="font-size:11px;">No data available</div>';
    return;
  }

  const syms = data.symbols || Object.keys(data.matrix);
  const short = s => s.replace('USDT', '');

  function corrBg(v) {
    if (v === 1)   return 'rgba(78,168,222,0.2)';
    if (v > 0.7)   return 'rgba(0,224,130,0.3)';
    if (v > 0.4)   return 'rgba(0,224,130,0.15)';
    if (v > -0.1)  return 'rgba(107,114,128,0.08)';
    if (v > -0.4)  return 'rgba(255,77,79,0.15)';
    return 'rgba(255,77,79,0.3)';
  }

  let html = '<table style="width:100%;border-collapse:collapse;font-size:10px;">';
  html += '<tr><th style="color:var(--muted);padding:2px 4px"></th>';
  syms.forEach(s => {
    html += `<th style="color:var(--muted);padding:2px 6px;text-align:center">${short(s)}</th>`;
  });
  html += '</tr>';
  syms.forEach(row => {
    html += `<tr><td style="color:var(--muted);padding:3px 4px;white-space:nowrap">${short(row)}</td>`;
    syms.forEach(col => {
      const v = data.matrix[row]?.[col];
      const vStr = v != null ? v.toFixed(2) : '—';
      const bg = v != null ? corrBg(v) : 'transparent';
      const fg = v === 1 ? 'var(--blue)' : (v != null && Math.abs(v) > 0.4 ? 'var(--fg)' : 'var(--muted)');
      html += `<td style="background:${bg};color:${fg};padding:3px 6px;text-align:center;border-radius:3px">${vStr}</td>`;
    });
    html += '</tr>';
  });
  html += '</table>';
  if (data.data_points != null) {
    html += `<div style="font-size:9px;color:var(--muted);margin-top:4px;text-align:right">${data.data_points} candles · ${Math.round((data.window || 3600) / 3600)}h</div>`;
  }
  el.innerHTML = html;
}

// ── Render: Volume Profile ─────────────────────────────────────────────────────
async function renderVolumeProfile() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/volume-profile?symbol=${sym}&window=3600`);
  const metricsEl = document.getElementById('volume-profile-metrics');

  if (!data?.bins?.length) {
    if (metricsEl) metricsEl.innerHTML = '<div class="text-muted" style="font-size:11px;">No data available</div>';
    return;
  }

  if (metricsEl) {
    metricsEl.innerHTML = `
      <span style="color:var(--muted)">POC <span style="color:var(--fg);font-weight:700">${fmtPrice(data.poc)}</span></span>
      <span style="color:var(--muted)">VAH <span style="color:var(--green)">${fmtPrice(data.vah)}</span></span>
      <span style="color:var(--muted)">VAL <span style="color:var(--red)">${fmtPrice(data.val)}</span></span>
      <span style="color:var(--muted)">Vol <span style="color:var(--fg)">${fmtK(data.total_volume)}</span></span>
      <span style="color:var(--muted)">VA <span style="color:var(--yellow)">${(data.value_area_pct || 70).toFixed(1)}%</span></span>
    `;
  }

  if (!volumeProfileChart) return;

  // Top 20 bins by volume, re-sorted by price (low→high for y-axis)
  const bins = [...data.bins]
    .sort((a, b) => b.volume - a.volume)
    .slice(0, 20)
    .sort((a, b) => a.price - b.price);

  volumeProfileChart.data.labels = bins.map(b => fmtPrice(b.price));
  volumeProfileChart.data.datasets[0].data = bins.map(b => b.buy_vol || 0);
  volumeProfileChart.data.datasets[1].data = bins.map(b => b.sell_vol || 0);
  volumeProfileChart.update('none');
}

// ── Render: Aggressor Ratio ────────────────────────────────────────────────────
async function renderAggressorRatio() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/aggressor-ratio?symbol=${sym}&window=1800&bucket=60`);
  const metricsEl = document.getElementById('aggressor-ratio-metrics');

  if (!data?.series?.length) {
    if (metricsEl) metricsEl.innerHTML = '<div class="text-muted" style="font-size:11px;">No data available</div>';
    return;
  }

  if (metricsEl) {
    const buyPct = data.rolling_buy_pct?.toFixed(1) ?? '—';
    const sellPct = data.rolling_sell_pct?.toFixed(1) ?? '—';
    const buyColor = parseFloat(buyPct) > 55 ? 'var(--green)' : parseFloat(buyPct) < 45 ? 'var(--red)' : 'var(--muted)';
    metricsEl.innerHTML = `
      <span style="color:var(--muted)">Buy <span style="color:var(--green);font-weight:700">${buyPct}%</span></span>
      <span style="color:var(--muted)">Sell <span style="color:var(--red)">${sellPct}%</span></span>
      <span style="color:var(--muted)">Signal <span style="color:${buyColor}">${data.signal || '—'}</span></span>
    `;
  }

  if (!aggressorChart) return;

  const src = data.series;
  aggressorChart.data.labels = src.map(d => fmtTime(d.ts));
  aggressorChart.data.datasets[0].data = src.map(d => d.buy_pct);
  aggressorChart.data.datasets[1].data = src.map(d => d.sell_pct);
  aggressorChart.update('none');
}

// ── Render: VPIN ──────────────────────────────────────────────────────────────
async function renderVpin() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/vpin?symbol=${sym}`);
  const el = document.getElementById('vpin-content');
  const badge = document.getElementById('vpin-badge');
  if (!el) return;

  if (!data || data.vpin == null) {
    el.innerHTML = '<div class="text-muted" style="font-size:11px;">No data</div>';
    if (badge) badge.style.display = 'none';
    return;
  }

  const vpin = data.vpin;
  const signal = data.signal || 'normal';
  const bucketsUsed = data.buckets_used ?? 0;

  const color = signal === 'elevated' ? 'var(--red)'
              : signal === 'low'      ? 'var(--green)'
              :                         'var(--muted)';

  const pct = (vpin * 100).toFixed(1) + '%';

  if (badge) {
    if (signal === 'elevated') {
      badge.textContent = 'elevated';
      badge.className = 'card-badge badge-red';
      badge.style.display = 'inline-block';
    } else if (signal === 'low') {
      badge.textContent = 'low';
      badge.className = 'card-badge badge-green';
      badge.style.display = 'inline-block';
    } else {
      badge.style.display = 'none';
    }
  }

  el.innerHTML = `
    <div class="metric-row">
      <span class="metric-label">VPIN</span>
      <span class="metric-value" style="color:${color};font-size:22px;font-weight:600">${pct}</span>
    </div>
    <div class="metric-row">
      <span class="metric-label">signal</span>
      <span class="metric-value" style="color:${color}">${signal}</span>
    </div>
    <div class="metric-row">
      <span class="metric-label">buckets</span>
      <span class="metric-value">${bucketsUsed}</span>
    </div>
  `;
}

// ── Render: Tape Speed ────────────────────────────────────────────────────────
async function renderTapeSpeed() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/tape-speed?symbol=${sym}`);
  const el = document.getElementById('tape-speed-content');
  if (!el) return;
  if (!data) { el.innerHTML = '<div class="text-muted" style="font-size:11px;">No data</div>'; return; }

  const cur    = data.current_tpm  != null ? data.current_tpm.toFixed(1)  : '—';
  const avg    = data.avg_tpm      != null ? data.avg_tpm.toFixed(1)      : '—';
  const high   = data.high_watermark != null ? data.high_watermark.toFixed(1) : '—';
  const low    = data.low_watermark  != null ? data.low_watermark.toFixed(1)  : '—';

  const heatColor = data.heating_up ? 'var(--red)' : data.cooling_down ? 'var(--blue)' : 'var(--fg)';
  const heatLabel = data.heating_up ? 'HEATING' : data.cooling_down ? 'COOLING' : 'STABLE';
  const badgeClass = data.heating_up ? 'badge-red' : data.cooling_down ? 'badge-blue' : 'badge-yellow';

  const badge = document.getElementById('tape-speed-badge');
  if (badge) {
    badge.textContent = heatLabel;
    badge.className = `card-badge ${badgeClass}`;
    badge.style.display = '';
  }

  // Bar chart: last N buckets as sparkline
  const buckets = (data.buckets || []).slice(-20);
  const maxTpm  = data.high_watermark || 1;
  const bars = buckets.map(b => {
    const pct = Math.min((b.tpm / maxTpm) * 100, 100).toFixed(1);
    return `<div class="ts-bar" style="height:${pct}%;background:var(--blue)" title="${b.tpm.toFixed(1)} tpm"></div>`;
  }).join('');

  el.innerHTML = `
    <div class="ts-metrics">
      <span class="ts-stat">Current <span style="color:${heatColor};font-weight:700">${cur} tpm</span></span>
      <span class="ts-stat">Avg <span>${avg} tpm</span></span>
      <span class="ts-stat">High <span class="text-yellow">${high}</span></span>
      <span class="ts-stat">Low <span class="text-muted">${low}</span></span>
    </div>
    <div class="ts-chart">${bars}</div>
  `;
}


async function renderAggressorStreak() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/aggressor-streak?symbol=${sym}`);
  if (!data) {
    setErr('aggressor-streak-content');
    return;
  }

  const el    = document.getElementById('aggressor-streak-content');
  const badge = document.getElementById('aggressor-streak-badge');
  if (!el) return;

  const streak    = data.streak || 0;
  const dir       = data.streak_direction;
  const alert     = data.alert;
  const threshold = data.alert_streak || 3;
  const candles   = data.candles || [];
  const desc      = data.description || '—';

  const dirColor = dir === 'buy' ? 'var(--green)' : dir === 'sell' ? 'var(--red)' : 'var(--muted)';
  const dirLabel = dir ? dir.toUpperCase() : '—';

  if (badge) {
    if (streak === 0) {
      badge.textContent  = 'no streak';
      badge.className    = 'card-badge badge-gray';
    } else if (alert) {
      badge.textContent  = `ALERT ${streak}x ${dirLabel}`;
      badge.className    = 'card-badge ' + (dir === 'buy' ? 'badge-green' : 'badge-red');
    } else {
      badge.textContent  = `${streak}x ${dirLabel}`;
      badge.className    = 'card-badge ' + (dir === 'buy' ? 'badge-green' : 'badge-red');
    }
    badge.style.display = 'inline-block';
  }

  // Last 10 candles as mini bar indicators
  const recent = candles.slice(-10);
  const bars = recent.map(c => {
    const col = c.direction === 'buy' ? 'var(--green)' : c.direction === 'sell' ? 'var(--red)' : 'var(--muted)';
    const pct = c.direction === 'buy' ? c.buy_pct.toFixed(0) : c.direction === 'sell' ? c.sell_pct.toFixed(0) : '—';
    return `<div class="metric-box" style="border-top:3px solid ${col}">
      <div class="metric-label">${new Date(c.ts * 1000).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})}</div>
      <div class="metric-value" style="color:${col};font-size:13px">${pct}%</div>
    </div>`;
  }).join('');

  el.innerHTML = `
    <div style="display:flex;gap:12px;align-items:center;margin-bottom:8px">
      <div>
        <div style="font-size:28px;font-weight:700;color:${dirColor}">${streak}</div>
        <div style="font-size:11px;color:var(--muted)">streak</div>
      </div>
      <div style="flex:1">
        <div style="font-size:13px;color:${alert ? dirColor : 'var(--fg)'}">
          ${alert ? '⚡ ' : ''}${desc}
        </div>
        <div style="font-size:11px;color:var(--muted);margin-top:2px">
          alert at ${threshold}+ consecutive candles · 70% threshold
        </div>
      </div>
    </div>
    <div class="phase-metrics" style="flex-wrap:wrap">${bars || '<span style="color:var(--muted);font-size:11px">No candle data</span>'}</div>
  `;
}

// ── Render: Correlation Heatmap ───────────────────────────────────────────────
async function renderCorrHeatmap() {
  const data = await apiFetch('/correlations/heatmap');
  const el = document.getElementById('corr-heatmap-content');
  if (!el) return;

  if (!data || !data.matrix || !data.symbols || data.matrix.length === 0) {
    el.innerHTML = '<div class="text-muted" style="font-size:11px;">No data available</div>';
    return;
  }

  const { symbols, matrix, quality } = data;
  const short = s => s.replace('USDT', '');

  // Color: red (1.0) → white (0.0) → blue (-1.0)
  function heatColor(v) {
    if (v == null || isNaN(v)) return 'rgba(107,114,128,0.1)';
    const c = Math.max(-1, Math.min(1, v));
    if (c >= 0) {
      const g = Math.round(255 * (1 - c));
      const b = Math.round(255 * (1 - c));
      return `rgb(255,${g},${b})`;
    } else {
      const t = -c;
      const r = Math.round(255 * (1 - t));
      const g = Math.round(255 * (1 - t));
      return `rgb(${r},${g},255)`;
    }
  }

  function textColor(v) {
    return (v != null && Math.abs(v) > 0.5) ? '#000' : 'var(--muted)';
  }

  let html = '<table style="width:100%;border-collapse:collapse;font-size:10px;">';
  html += '<tr><th style="color:var(--muted);padding:2px 4px"></th>';
  symbols.forEach(s => {
    html += `<th style="color:var(--muted);padding:2px 6px;text-align:center">${short(s)}</th>`;
  });
  html += '</tr>';

  matrix.forEach((row, i) => {
    html += `<tr><td style="color:var(--muted);padding:3px 4px;white-space:nowrap">${short(symbols[i])}</td>`;
    row.forEach(v => {
      const bg = heatColor(v);
      const fg = textColor(v);
      const vStr = v != null ? v.toFixed(2) : '—';
      html += `<td style="background:${bg};color:${fg};padding:3px 6px;text-align:center;border-radius:3px">${vStr}</td>`;
    });
    html += '</tr>';
  });
  html += '</table>';
  html += `<div style="font-size:9px;color:var(--muted);margin-top:4px;text-align:right">${quality} periods · returns</div>`;
  el.innerHTML = html;

  const badge = document.getElementById('corr-heatmap-badge');
  if (badge) {
    badge.style.display = '';
    badge.textContent = `${quality}p`;
    badge.className = 'card-badge ' + (quality >= 15 ? 'badge-green' : quality >= 5 ? 'badge-yellow' : 'badge-red');
  }
}

// ── OB Walls ──────────────────────────────────────────────────────────────────
async function renderObWalls() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/ob-walls?symbol=${sym}`);
  const el = document.getElementById('ob-walls-content');
  const badge = document.getElementById('ob-walls-badge');
  if (!el) return;

  if (!data) {
    setErr('ob-walls-content');
    return;
  }

  const walls = data.walls || [];
  const risk = data.liquidation_risk || 'low';

  if (badge) {
    badge.textContent = risk + ' risk';
    badge.className = 'card-badge ' + (
      risk === 'high' ? 'badge-red' : risk === 'medium' ? 'badge-yellow' : 'badge-blue'
    );
    badge.style.display = 'inline-block';
  }

  if (walls.length === 0) {
    el.innerHTML = `<div style="color:var(--muted);font-size:11px;padding:8px 0">No walls detected · median ${data.median_size != null ? data.median_size.toFixed(2) : '—'}</div>`;
    return;
  }

  function wallColor(d) { return d < 5 ? 'var(--red)' : d < 20 ? 'var(--yellow)' : 'var(--green)'; }
  function wallLabel(d) { return d < 5 ? 'solid' : d < 20 ? 'weakening' : 'breaking'; }
  function fmtAge(s) { return s < 60 ? s + 's' : Math.floor(s/60) + 'm' + String(s%60).padStart(2,'0') + 's'; }

  const rows = walls.map(w => {
    const col = wallColor(w.decay_pct);
    const sideCol = w.side === 'bid' ? 'var(--green)' : 'var(--red)';
    const decayBar = Math.min(100, w.decay_pct);
    return `<tr>
      <td style="color:${sideCol};font-weight:600;padding:3px 6px 3px 0">${w.side.toUpperCase()}</td>
      <td style="font-family:monospace;padding:3px 6px 3px 0">${typeof w.price === 'number' ? w.price.toFixed(6) : w.price}</td>
      <td style="font-family:monospace;padding:3px 6px 3px 0">${w.size.toLocaleString(undefined,{maximumFractionDigits:2})}</td>
      <td style="color:var(--muted);font-size:11px;padding:3px 6px 3px 0">${fmtAge(w.age_sec)}</td>
      <td style="padding:3px 0">
        <span style="color:${col};font-size:11px">${wallLabel(w.decay_pct)}</span>
        <div style="background:var(--bg2);border-radius:2px;height:4px;width:50px;display:inline-block;vertical-align:middle;margin-left:4px">
          <div style="background:${col};width:${decayBar}%;height:100%;border-radius:2px"></div>
        </div>
        <span style="color:var(--muted);font-size:10px;margin-left:4px">${w.decay_pct.toFixed(1)}%</span>
      </td>
    </tr>`;
  }).join('');

  el.innerHTML = `
    <div style="font-size:10px;color:var(--muted);margin-bottom:6px">threshold: ${data.wall_threshold != null ? data.wall_threshold.toFixed(2) : '—'} · median: ${data.median_size != null ? data.median_size.toFixed(2) : '—'}</div>
    <table style="width:100%;border-collapse:collapse;font-size:12px">
      <thead><tr style="color:var(--muted);font-size:10px;text-align:left">
        <th style="padding:2px 6px 4px 0">side</th><th style="padding:2px 6px 4px 0">price</th>
        <th style="padding:2px 6px 4px 0">size</th><th style="padding:2px 6px 4px 0">age</th>
        <th style="padding:2px 0 4px 0">decay</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

// ── Top Movers ────────────────────────────────────────────────────────────────
async function renderTopMovers() {
  const data = await apiFetch('/top-movers');
  const el = document.getElementById('top-movers-content');
  const badge = document.getElementById('top-movers-badge');
  if (!el) return;

  if (!data) {
    setErr('top-movers-content');
    return;
  }

  const movers = data.movers || [];

  if (movers.length === 0) {
    el.innerHTML = '<div style="color:var(--muted);font-size:11px;padding:8px 0">No data</div>';
    return;
  }

  function fmtPct(v) {
    if (v == null) return '<span style="color:var(--muted)">—</span>';
    const sign = v >= 0 ? '+' : '';
    const col = v > 0 ? 'var(--green)' : v < 0 ? 'var(--red)' : 'var(--muted)';
    return `<span style="color:${col}">${sign}${v.toFixed(2)}%</span>`;
  }
  function fmtPrice(v) {
    if (v == null) return '<span style="color:var(--muted)">—</span>';
    // Use enough decimals for sub-penny assets
    const dec = v < 0.01 ? 6 : v < 1 ? 4 : 2;
    return v.toFixed(dec);
  }

  const top = movers[0];
  if (badge && top) {
    const ch = top.change_1h;
    badge.textContent = top.symbol.replace('USDT', '');
    badge.className = 'card-badge ' + (ch == null ? 'badge-blue' : ch > 0 ? 'badge-green' : 'badge-red');
    badge.style.display = 'inline-block';
  }

  const rows = movers.map(m => `<tr>
    <td style="font-weight:600;padding:3px 6px 3px 0;font-size:11px">${m.symbol.replace('USDT','')}</td>
    <td style="font-family:monospace;padding:3px 6px 3px 0;font-size:11px;color:var(--muted)">${fmtPrice(m.price)}</td>
    <td style="text-align:right;padding:3px 6px 3px 0;font-size:11px">${fmtPct(m.change_1h)}</td>
    <td style="text-align:right;padding:3px 6px 3px 0;font-size:11px">${fmtPct(m.change_4h)}</td>
    <td style="text-align:right;padding:3px 0;font-size:11px">${fmtPct(m.change_24h)}</td>
  </tr>`).join('');

  el.innerHTML = `
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.05em">
        <th style="text-align:left;padding:2px 6px 4px 0;font-weight:400">symbol</th>
        <th style="text-align:left;padding:2px 6px 4px 0;font-weight:400">price</th>
        <th style="text-align:right;padding:2px 6px 4px 0;font-weight:400">1h</th>
        <th style="text-align:right;padding:2px 6px 4px 0;font-weight:400">4h</th>
        <th style="text-align:right;padding:2px 0 4px 0;font-weight:400">24h</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

// ── Main Refresh Loop ─────────────────────────────────────────────────────────
async function refresh() {
  if (!activeSymbol) return;
  const safe = fn => fn().catch(e => console.warn('[refresh]', fn.name, e.message));

  // Batch 1: core charts + header stats
  await Promise.all([
    safe(renderPriceChart),
    safe(renderOiChart),
    safe(renderCvdChart),
    safe(renderFunding),
    safe(renderFundingMomentum),
    safe(renderSpread),
    safe(renderWsStats),
  ]);
  // Batch 2: trade data
  await Promise.all([
    safe(renderTradeTape),
    safe(renderVolumeImbalance),
    safe(renderPhase),
    safe(renderOiDivergence),
    safe(renderMicrostructure),
  ]);
  // Batch 3: advanced
  await Promise.all([
    safe(renderWhaleClustering),
    safe(renderVwapDeviation),
    safe(renderOiWeightedPrice),
    safe(renderRealizedVolBands),
    safe(renderMarketRegime),
    safe(renderMomentum),
    safe(renderRegimeTimeline),
  ]);
  // Batch 4: secondary
  await Promise.all([
    safe(renderCorrelations),
    safe(renderCorrHeatmap),
    safe(renderVolumeProfile),
    safe(renderAggressorRatio),
    safe(renderVpin),
    safe(renderAdaptiveVolumeProfile),
    safe(renderTapeSpeed),
    safe(renderAggressorStreak),
    safe(renderObWalls),
    safe(renderTopMovers),
  ]);
}

// ── Bootstrap ─────────────────────────────────────────────────────────────────
async function init() {
  const safeInit = (fn) => { try { fn(); } catch(e) { console.warn('Chart init failed:', e.message); } };
  safeInit(initPriceChart);
  safeInit(initOiChart);
  safeInit(initCvdChart);
  safeInit(initFundingChart);
  safeInit(initSpreadChart);
  safeInit(initAggressorChart);
  safeInit(initVolumeProfileChart);
  safeInit(initRegimeTimelineChart);
  safeInit(initAdaptiveVpChart);
  initTheme();
  connectAlerts();

  // After 10s replace any still-Loading cards with Error badge
  setTimeout(() => {
    document.querySelectorAll('[id$="-content"]').forEach(el => {
      const txt = el.textContent.trim();
      if (txt.startsWith('Loading') || txt === 'No data available') {
        el.innerHTML = '<span class="card-badge badge-red" style="display:inline-block">Error</span>';
      }
    });
  }, 10000);

  await loadSymbols();
  await refresh();

  refreshTimer = setInterval(refresh, REFRESH_MS);
}


document.addEventListener('DOMContentLoaded', init);

