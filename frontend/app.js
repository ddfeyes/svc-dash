'use strict';

// ── Config ────────────────────────────────────────────────────────────────────
// Use same host for API (nginx will proxy /api to backend:8765)
const API = window.location.protocol + '//' + window.location.host + '/api';
const WS  = (window.location.protocol === 'https:' ? 'wss://' : 'ws://') + window.location.host;

const REFRESH_MS   = 15000;  // poll interval (15s to avoid backend overload)
const TRADE_MAX    = 100;    // max rows in tape


const ALERT_MAX    = 50;     // max rows in alerts feed
const WHALE_USD    = 10000;  // highlight threshold

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

// ── Liquidation Heatmap ───────────────────────────────────────────────────────
async function renderLiqHeatmap() {
  const data = await apiFetch('/liquidation-heatmap?window_s=3600&buckets=20');
  const el    = document.getElementById('liq-heatmap-content');
  const badge = document.getElementById('liq-heatmap-badge');
  if (!el) return;

  if (!data) { setErr('liq-heatmap-content'); return; }

  const syms = Object.keys(data.symbols || {});
  if (syms.length === 0) {
    el.innerHTML = '<div style="color:var(--muted);font-size:11px;padding:8px 0">No liquidation data</div>';
    return;
  }

  // Compute cross-symbol max for consistent colour scale
  let globalMax = 0;
  for (const s of syms) {
    const entry = data.symbols[s];
    for (const b of (entry.buckets || [])) globalMax = Math.max(globalMax, b.total_usd);
  }

  function liqColor(totalUsd, longUsd, shortUsd) {
    if (totalUsd <= 0 || globalMax <= 0) return 'var(--bg3)';
    const intensity = Math.log1p(totalUsd) / Math.log1p(globalMax);
    const alpha = Math.max(0.08, intensity);
    // long liquidations = price was dropping (forced longs out) → red
    // short liquidations = price was rising (forced shorts out) → green
    if (longUsd >= shortUsd) return `rgba(255,77,79,${alpha.toFixed(2)})`;
    return `rgba(0,224,130,${alpha.toFixed(2)})`;
  }

  function fmtUsd(v) {
    if (v >= 1e6) return '$' + (v / 1e6).toFixed(1) + 'M';
    if (v >= 1e3) return '$' + (v / 1e3).toFixed(1) + 'k';
    return '$' + v.toFixed(0);
  }

  // Badge: total liquidated across all symbols
  const grandTotal = syms.reduce((s, k) => s + (data.symbols[k].total_usd || 0), 0);
  if (badge) {
    badge.textContent = fmtUsd(grandTotal);
    badge.className = 'card-badge ' + (grandTotal > 50000 ? 'badge-red' : grandTotal > 5000 ? 'badge-yellow' : 'badge-blue');
    badge.style.display = 'inline-block';
  }

  let html = '';
  for (const sym of syms) {
    const entry = data.symbols[sym];
    const buckets = entry.buckets || [];
    const label = sym.replace('USDT', '');
    const symTotal = entry.total_usd || 0;
    const nLiqs = entry.n_liquidations || 0;

    html += `<div style="margin-bottom:10px">`;
    html += `<div style="display:flex;justify-content:space-between;font-size:10px;color:var(--muted);margin-bottom:3px">`;
    html += `<span style="font-weight:600;color:var(--fg)">${label}</span>`;
    html += `<span>${nLiqs} liqs · ${fmtUsd(symTotal)}</span></div>`;

    if (buckets.length === 0) {
      html += `<div style="font-size:10px;color:var(--muted);padding:4px 0">No liquidations in window</div>`;
    } else {
      html += `<div style="display:flex;flex-direction:column;gap:1px">`;
      // Render bucket strip: reversed so high price is on the right
      const reversed = [...buckets].reverse();
      html += `<div style="display:flex;gap:1px;height:28px;align-items:stretch">`;
      for (const b of reversed) {
        const col = liqColor(b.total_usd, b.long_usd, b.short_usd);
        const tip = b.total_usd > 0
          ? `${fmtUsd(b.total_usd)} (L:${fmtUsd(b.long_usd)} S:${fmtUsd(b.short_usd)})`
          : '';
        html += `<div title="${tip}" style="flex:1;background:${col};border-radius:1px;min-width:2px"></div>`;
      }
      html += `</div>`;
      // Price axis: low price left, high price right
      if (entry.price_min != null) {
        const dec = entry.price_min < 0.01 ? 6 : entry.price_min < 1 ? 4 : 2;
        html += `<div style="display:flex;justify-content:space-between;font-size:9px;color:var(--muted);margin-top:2px">`;
        html += `<span>${entry.price_min.toFixed(dec)}</span><span>${entry.price_max.toFixed(dec)}</span></div>`;
      }
      html += `</div>`;
    }
    html += `</div>`;
  }

  // Legend
  html += `<div style="display:flex;gap:12px;font-size:9px;color:var(--muted);margin-top:4px">`;
  html += `<span><span style="color:var(--red)">■</span> longs liq'd</span>`;
  html += `<span><span style="color:var(--green)">■</span> shorts liq'd</span>`;
  html += `<span style="margin-left:auto">darker = larger</span>`;
  html += `</div>`;

  el.innerHTML = html;
}

// ── CVD Momentum ─────────────────────────────────────────────────────────────
async function renderCvdMomentum() {
  const sym  = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/cvd-momentum?symbol=${sym}`);
  const el   = document.getElementById('cvd-momentum-content');
  const badge = document.getElementById('cvd-momentum-badge');
  if (!el) return;
  if (!data) { setErr('cvd-momentum-content'); return; }

  const dir   = data.direction || 'neutral';
  const bCls  = dir === 'bullish' ? 'badge-green' : dir === 'bearish' ? 'badge-red' : 'badge-blue';
  const bLbl  = dir.toUpperCase();
  if (badge) { badge.textContent = bLbl; badge.className = `card-badge ${bCls}`; badge.style.display = ''; }

  const rate   = data.cvd_rate;
  const intens = Math.max(0, Math.min(100, Math.round((data.intensity ?? 0) * 100)));
  const intCol = intens > 70 ? 'var(--red)' : intens > 40 ? 'var(--yellow)' : 'var(--green)';
  const accel  = data.accelerating ? '▲ accel' : '▼ decel';
  const accelCol = data.accelerating ? 'var(--green)' : 'var(--muted)';

  function fmtRate(r) {
    if (r == null) return '—';
    const abs = Math.abs(r);
    const s   = r < 0 ? '-' : '+';
    if (abs >= 1e6) return `${s}$${(abs/1e6).toFixed(2)}M/s`;
    if (abs >= 1e3) return `${s}$${(abs/1e3).toFixed(1)}k/s`;
    return `${s}$${abs.toFixed(2)}/s`;
  }

  el.innerHTML = `
    <div style="display:flex;gap:16px;flex-wrap:wrap;font-size:11px;margin-bottom:8px">
      <span style="color:var(--muted)">Rate <span style="color:var(--fg);font-weight:700">${fmtRate(rate)}</span></span>
      <span style="color:${accelCol};font-weight:600">${accel}</span>
    </div>
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
      <span style="font-size:10px;color:var(--muted);min-width:60px">Intensity</span>
      <div style="flex:1;background:var(--bg3);border-radius:3px;height:8px">
        <div style="background:${intCol};width:${intens}%;height:100%;border-radius:3px;transition:width .3s"></div>
      </div>
      <span style="font-size:10px;color:${intCol};min-width:32px;text-align:right">${intens}%</span>
    </div>
    <div style="font-size:10px;color:var(--muted);margin-top:4px">
      Total CVD: <span style="color:var(--fg)">${data.cvd_total_usd != null ? '$' + data.cvd_total_usd.toLocaleString(undefined,{maximumFractionDigits:0}) : '—'}</span>
      · window: <span style="color:var(--fg)">${data.window_seconds}s</span>
    </div>`;
}

// ── Delta Divergence ──────────────────────────────────────────────────────────
async function renderDeltaDivergence() {
  const sym  = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/delta-divergence?symbol=${sym}`);
  const el   = document.getElementById('delta-divergence-content');
  const badge = document.getElementById('delta-divergence-badge');
  if (!el) return;
  if (!data) { setErr('delta-divergence-content'); return; }

  const sev  = data.severity ?? 0;
  const div  = data.divergence || 'none';
  const bCls = sev === 0 ? 'badge-green' : sev === 1 ? 'badge-yellow' : 'badge-red';
  const bLbl = sev === 0 ? 'OK' : div.toUpperCase();
  if (badge) { badge.textContent = bLbl; badge.className = `card-badge ${bCls}`; badge.style.display = ''; }

  function fmtPct(v) {
    if (v == null) return '—';
    const sign = v > 0 ? '+' : '';
    const col  = v > 0 ? 'var(--green)' : v < 0 ? 'var(--red)' : 'var(--muted)';
    return `<span style="color:${col}">${sign}${v.toFixed(2)}%</span>`;
  }

  el.innerHTML = `
    <div style="display:flex;gap:16px;flex-wrap:wrap;font-size:11px;margin-bottom:8px">
      <span style="color:var(--muted)">Price Δ ${fmtPct(data.price_change_pct)}</span>
      <span style="color:var(--muted)">CVD norm <span style="color:var(--fg);font-weight:600">${data.cvd_norm != null ? data.cvd_norm.toFixed(3) : '—'}</span></span>
    </div>
    <div style="font-size:11px;color:var(--fg);line-height:1.4">${data.description || '—'}</div>
    <div style="font-size:10px;color:var(--muted);margin-top:4px">window: ${data.window_seconds}s</div>`;
}

// ── Funding Extreme ───────────────────────────────────────────────────────────
async function renderFundingExtreme() {
  const sym  = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/funding-extreme?symbol=${sym}`);
  const el   = document.getElementById('funding-extreme-content');
  const badge = document.getElementById('funding-extreme-badge');
  if (!el) return;
  if (!data) { setErr('funding-extreme-content'); return; }

  const isExt = !!data.extreme;
  const bLbl  = isExt ? 'EXTREME' : 'normal';
  const bCls  = isExt ? 'badge-red' : 'badge-blue';
  if (badge) { badge.textContent = bLbl; badge.className = `card-badge ${bCls}`; badge.style.display = ''; }

  function fmtRatePct(v) {
    if (v == null) return '—';
    const sign = v >= 0 ? '+' : '';
    return `${sign}${v.toFixed(4)}%`;
  }

  const rates = data.rates || {};
  const rateRows = Object.entries(rates).map(([exch, r]) =>
    `<span style="color:var(--muted)">${exch} <span style="color:var(--fg);font-weight:600">${fmtRatePct(r != null ? r * 100 : null)}</span></span>`
  ).join('');

  const dirLbl = data.direction ? data.direction.toUpperCase() + ' paying' : '';
  const dirCol = data.direction === 'long' ? 'var(--red)' : data.direction === 'short' ? 'var(--green)' : 'var(--muted)';

  el.innerHTML = `
    <div style="font-size:16px;font-weight:700;color:${isExt ? 'var(--red)' : 'var(--fg)'};margin-bottom:6px">
      ${fmtRatePct(data.avg_rate_pct)}
      ${dirLbl ? `<span style="font-size:11px;font-weight:400;color:${dirCol};margin-left:8px">${dirLbl}</span>` : ''}
    </div>
    <div style="display:flex;gap:12px;flex-wrap:wrap;font-size:11px;margin-bottom:6px">${rateRows}</div>
    <div style="font-size:11px;color:var(--muted)">${data.description || '—'}</div>`;
}

// ── Liq Cascade ───────────────────────────────────────────────────────────────
async function renderLiqCascade() {
  const sym  = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/liq-cascade?symbol=${sym}`);
  const el   = document.getElementById('liq-cascade-content');
  const badge = document.getElementById('liq-cascade-badge');
  if (!el) return;
  if (!data) { setErr('liq-cascade-content'); return; }

  const isCascade = !!data.cascade;
  const bLbl = isCascade ? 'CASCADE' : 'quiet';
  const bCls = isCascade ? 'badge-red' : 'badge-blue';
  if (badge) { badge.textContent = bLbl; badge.className = `card-badge ${bCls}`; badge.style.display = ''; }

  function fmtUsd(v) {
    if (!v) return '$0';
    if (v >= 1e6) return '$' + (v/1e6).toFixed(2) + 'M';
    if (v >= 1e3) return '$' + (v/1e3).toFixed(1) + 'k';
    return '$' + v.toFixed(0);
  }

  const total = data.total_usd || 0;
  const buyP  = total > 0 ? Math.min(100, Math.round(data.buy_usd  / total * 100)) : 0;
  const selP  = total > 0 ? Math.min(100, Math.round(data.sell_usd / total * 100)) : 0;

  el.innerHTML = `
    <div style="font-size:${isCascade ? '18px' : '14px'};font-weight:700;color:${isCascade ? 'var(--red)' : 'var(--fg)'};margin-bottom:8px">
      ${fmtUsd(total)}
    </div>
    <div style="display:flex;gap:8px;font-size:11px;margin-bottom:6px">
      <span style="color:var(--green)">↑ Buy ${fmtUsd(data.buy_usd)}</span>
      <span style="color:var(--muted)">·</span>
      <span style="color:var(--red)">↓ Sell ${fmtUsd(data.sell_usd)}</span>
    </div>
    <div style="display:flex;gap:2px;height:8px;border-radius:4px;overflow:hidden;background:var(--bg3)">
      <div style="width:${buyP}%;background:var(--green);transition:width .3s"></div>
      <div style="width:${selP}%;background:var(--red);transition:width .3s"></div>
    </div>
    <div style="font-size:10px;color:var(--muted);margin-top:6px">${data.description || '—'}</div>`;
}

// ── Large Trades ──────────────────────────────────────────────────────────────
async function renderLargeTrades() {
  const sym  = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/large-trades?symbol=${sym}&limit=8`);
  const el   = document.getElementById('large-trades-content');
  const badge = document.getElementById('large-trades-badge');
  if (!el) return;
  if (!data) { setErr('large-trades-content'); return; }

  const trades = data.trades || [];
  const count  = data.count || 0;
  if (badge) {
    badge.textContent = count + ' whales';
    badge.className = `card-badge ${count > 0 ? 'badge-yellow' : 'badge-blue'}`;
    badge.style.display = '';
  }

  if (trades.length === 0) {
    el.innerHTML = `<div style="color:var(--muted);font-size:11px;padding:8px 0">No large trades in window</div>`;
    return;
  }

  function fmtUsd(v) {
    if (v >= 1e6) return '$' + (v/1e6).toFixed(2) + 'M';
    if (v >= 1e3) return '$' + (v/1e3).toFixed(1) + 'k';
    return '$' + v.toFixed(0);
  }
  function fmtTs(ts) {
    const d = new Date(ts * 1000);
    return d.toTimeString().slice(0,8);
  }
  function fmtPrice(p) {
    return p < 0.01 ? p.toFixed(6) : p < 1 ? p.toFixed(4) : p.toFixed(2);
  }

  const buyUsd  = data.total_buy_usd  || 0;
  const sellUsd = data.total_sell_usd || 0;

  const rows = trades.map(t => {
    const sideCol = t.side === 'buy' ? 'var(--green)' : 'var(--red)';
    const sideLbl = t.side.toUpperCase();
    return `<tr style="border-bottom:1px solid var(--border)">
      <td style="padding:3px 6px 3px 0"><span style="color:${sideCol};font-weight:700;font-size:10px">${sideLbl}</span></td>
      <td style="padding:3px 6px;font-family:monospace;font-size:11px">${fmtPrice(t.price)}</td>
      <td style="padding:3px 6px;text-align:right;font-size:11px;color:var(--yellow);font-weight:600">${fmtUsd(t.usd_value)}</td>
      <td style="padding:3px 0;text-align:right;font-size:10px;color:var(--muted)">${fmtTs(t.ts)}</td>
    </tr>`;
  }).join('');

  el.innerHTML = `
    <div style="display:flex;gap:16px;font-size:11px;margin-bottom:8px">
      <span style="color:var(--muted)">Buy <span style="color:var(--green);font-weight:700">${fmtUsd(buyUsd)}</span></span>
      <span style="color:var(--muted)">Sell <span style="color:var(--red);font-weight:700">${fmtUsd(sellUsd)}</span></span>
    </div>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="color:var(--muted);font-size:9px;text-transform:uppercase;letter-spacing:.05em">
        <th style="text-align:left;padding:2px 6px 4px 0;font-weight:400">side</th>
        <th style="text-align:left;padding:2px 6px 4px;font-weight:400">price</th>
        <th style="text-align:right;padding:2px 6px 4px;font-weight:400">value</th>
        <th style="text-align:right;padding:2px 0 4px;font-weight:400">time</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

// ── Net Taker Delta ───────────────────────────────────────────────────────────
// ── Alerts ────────────────────────────────────────────────────────────────────
async function renderAlerts() {
  const el    = document.getElementById('alerts-content');
  const badge = document.getElementById('alerts-badge');
  if (!el) return;
  const data = await apiFetch('/alerts?limit=20');
  if (!data) { setErr('alerts-content'); return; }

  const count = data.count || 0;
  if (badge) {
    badge.textContent = count > 0 ? String(count) : '0';
    badge.className = `card-badge ${count > 0 ? 'badge-red' : 'badge-blue'}`;
    badge.style.display = '';
  }

  if (!count || !data.data || !data.data.length) {
    el.innerHTML = '<div class="text-muted" style="font-size:11px;">No active alerts</div>';
    return;
  }

  const rows = data.data.slice(0, 10).map(a => {
    const sev = a.severity || 'info';
    const sevCls = sev === 'critical' ? 'badge-red' : sev === 'warning' ? 'badge-yellow' : 'badge-blue';
    return `<div style="margin-bottom:4px;font-size:11px;">
      <span class="card-badge ${sevCls}" style="font-size:9px;margin-right:4px;">${sev}</span>
      <span style="color:var(--muted);margin-right:4px;">${(a.symbol||'').replace('USDT','')}</span>
      <span>${a.message || a.type || ''}</span>
    </div>`;
  }).join('');
  el.innerHTML = rows;
}

// ── OI Delta ──────────────────────────────────────────────────────────────────
async function renderOiDelta() {
  const sym   = encodeURIComponent(activeSymbol);
  const el    = document.getElementById('oi-delta-content');
  const badge = document.getElementById('oi-delta-badge');
  if (!el) return;
  const data = await apiFetch(`/oi-delta?symbol=${sym}&interval=300&window=3600`);
  if (!data) { setErr('oi-delta-content'); return; }

  const candles = data.candles || [];
  const totalChange = candles.reduce((sum, c) => sum + (c.oi_change || 0), 0);

  const dir = totalChange > 0 ? 'up' : totalChange < 0 ? 'down' : 'flat';
  const dirCls = totalChange > 0 ? 'badge-green' : totalChange < 0 ? 'badge-red' : 'badge-blue';
  if (badge) {
    badge.textContent = dir;
    badge.className = `card-badge ${dirCls}`;
    badge.style.display = '';
  }

  if (!candles.length) {
    el.innerHTML = '<div class="text-muted" style="font-size:11px;">No OI data</div>';
    return;
  }

  function fmtOi(v) {
    const abs = Math.abs(v);
    if (abs >= 1e6) return (v >= 0 ? '+' : '') + (v / 1e6).toFixed(2) + 'M';
    if (abs >= 1e3) return (v >= 0 ? '+' : '') + (v / 1e3).toFixed(1) + 'k';
    return (v >= 0 ? '+' : '') + v.toFixed(0);
  }

  const totalCol = totalChange >= 0 ? 'var(--green)' : 'var(--red)';
  const recent = candles.slice(-5);
  const sparkRows = recent.map(c => {
    const col = c.oi_change >= 0 ? 'var(--green)' : 'var(--red)';
    return `<span style="color:${col};font-size:10px;margin-right:6px;">${fmtOi(c.oi_change)}</span>`;
  }).join('');

  el.innerHTML = `
    <div style="font-size:18px;font-weight:700;color:${totalCol};margin-bottom:6px">${fmtOi(totalChange)}</div>
    <div style="font-size:10px;color:var(--muted);margin-bottom:4px">last ${recent.length} candles:</div>
    <div style="flex-wrap:wrap">${sparkRows}</div>`;
}

// ── Squeeze Setup ─────────────────────────────────────────────────────────────
async function renderSqueezeSetup() {
  const sym   = encodeURIComponent(activeSymbol);
  const el    = document.getElementById('squeeze-setup-content');
  const badge = document.getElementById('squeeze-setup-badge');
  if (!el) return;
  const data = await apiFetch(`/squeeze-setup?symbol=${sym}`);
  if (!data) { setErr('squeeze-setup-content'); return; }

  const isSqueeze = !!data.squeeze_signal;
  const bLbl = isSqueeze ? 'SQUEEZE' : 'off';
  const bCls = isSqueeze ? 'badge-red' : 'badge-blue';
  if (badge) {
    badge.textContent = bLbl;
    badge.className = `card-badge ${bCls}`;
    badge.style.display = '';
  }

  function fmtRate(v) {
    if (v == null) return '—';
    const sign = v >= 0 ? '+' : '';
    return `${sign}${(v * 100).toFixed(4)}%`;
  }

  const oiSurge = data.oi_surge_with_crash;
  const fundNorm = data.funding_normalizing;
  const signals = [
    `<span style="color:${oiSurge ? 'var(--green)' : 'var(--muted)'}">OI surge+crash: ${oiSurge ? 'yes' : 'no'}</span>`,
    `<span style="color:${fundNorm ? 'var(--green)' : 'var(--muted)'}">Funding normalizing: ${fundNorm ? 'yes' : 'no'}</span>`,
  ].join('<span style="color:var(--muted);margin:0 6px">·</span>');

  el.innerHTML = `
    <div style="font-size:${isSqueeze ? '16px' : '13px'};font-weight:700;color:${isSqueeze ? 'var(--red)' : 'var(--muted)'};margin-bottom:6px">
      ${isSqueeze ? 'SQUEEZE SETUP' : 'No squeeze signal'}
    </div>
    <div style="font-size:11px;margin-bottom:6px">${signals}</div>
    ${data.funding_start != null ? `<div style="font-size:10px;color:var(--muted)">Funding: ${fmtRate(data.funding_start)} → ${fmtRate(data.funding_end)}</div>` : ''}
    <div style="font-size:10px;color:var(--muted);margin-top:4px">${data.description || ''}</div>`;
}

// ── Volume Spike ──────────────────────────────────────────────────────────────
async function renderVolumeSpikeCard() {
  const sym   = encodeURIComponent(activeSymbol);
  const el    = document.getElementById('volume-spike-content');
  const badge = document.getElementById('volume-spike-badge');
  if (!el) return;
  const data = await apiFetch(`/volume-spike?symbol=${sym}`);
  if (!data) { setErr('volume-spike-content'); return; }

  const isSpike = !!data.spike;
  const bLbl = isSpike ? 'SPIKE' : 'normal';
  const bCls = isSpike ? 'badge-red' : 'badge-blue';
  if (badge) {
    badge.textContent = bLbl;
    badge.className = `card-badge ${bCls}`;
    badge.style.display = '';
  }

  function fmtUsdK(v) {
    if (!v) return '$0';
    if (v >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'M';
    if (v >= 1e3) return '$' + (v / 1e3).toFixed(1) + 'k';
    return '$' + v.toFixed(0);
  }

  const ratio = (data.ratio || 0).toFixed(2);
  const dominant = (data.dominant || '—').toUpperCase();
  const domCls = dominant === 'BUY' ? 'var(--green)' : dominant === 'SELL' ? 'var(--red)' : 'var(--muted)';
  const domPct = (data.dominant_pct || 0).toFixed(1);

  el.innerHTML = `
    <div style="font-size:18px;font-weight:700;color:${isSpike ? 'var(--red)' : 'var(--fg)'};margin-bottom:6px">
      ${ratio}× <span style="font-size:12px;font-weight:400;color:var(--muted)">vs baseline</span>
    </div>
    <div style="display:flex;gap:12px;font-size:11px;margin-bottom:4px">
      <span style="color:var(--muted)">Recent: <span style="color:var(--fg);font-weight:600">${fmtUsdK(data.recent_usd)}</span></span>
      <span style="color:var(--muted)">Baseline: <span style="color:var(--fg)">${fmtUsdK(data.baseline_usd_per_period)}</span></span>
    </div>
    <div style="font-size:11px">
      Dominant: <span style="color:${domCls};font-weight:700">${dominant}</span>
      <span style="color:var(--muted);margin-left:4px">${domPct}%</span>
    </div>`;
}

// ── Trade Count Rate ──────────────────────────────────────────────────────────
async function renderTradeCountRate() {
  const sym   = encodeURIComponent(activeSymbol);
  const el    = document.getElementById('trade-count-rate-content');
  const badge = document.getElementById('trade-count-rate-badge');
  if (!el) return;
  const data = await apiFetch(`/trade-count-rate?symbol=${sym}&interval=60&window=1800`);
  if (!data) { setErr('trade-count-rate-content'); return; }

  const buckets = data.buckets || [];
  const currentTpm = buckets.length ? buckets[buckets.length - 1].trades_per_min : 0;

  // compute trend from first half vs second half
  let trend = 'flat';
  if (buckets.length >= 2) {
    const mid = Math.floor(buckets.length / 2);
    const firstHalf  = buckets.slice(0, mid);
    const secondHalf = buckets.slice(mid);
    const avgFirst  = firstHalf.reduce((s, b) => s + b.trades_per_min, 0) / firstHalf.length;
    const avgSecond = secondHalf.reduce((s, b) => s + b.trades_per_min, 0) / secondHalf.length;
    if (avgFirst > 0) {
      const pctChange = (avgSecond - avgFirst) / avgFirst;
      if (pctChange > 0.10)       trend = 'rising';
      else if (pctChange < -0.10) trend = 'falling';
    }
  }

  const trendArrow = trend === 'rising' ? '↑' : trend === 'falling' ? '↓' : '→';
  const trendCol   = trend === 'rising' ? 'var(--green)' : trend === 'falling' ? 'var(--red)' : 'var(--muted)';

  if (badge) {
    badge.textContent = trend;
    badge.className = `card-badge ${trend === 'rising' ? 'badge-green' : trend === 'falling' ? 'badge-red' : 'badge-blue'}`;
    badge.style.display = '';
  }

  if (!buckets.length) {
    el.innerHTML = '<div class="text-muted" style="font-size:11px;">No trade rate data</div>';
    return;
  }

  el.innerHTML = `
    <div style="font-size:18px;font-weight:700;color:var(--fg);margin-bottom:6px">
      ${currentTpm.toFixed(1)} <span style="font-size:12px;font-weight:400;color:var(--muted)">trades/min</span>
      <span style="font-size:16px;color:${trendCol};margin-left:6px">${trendArrow}</span>
    </div>
    <div style="font-size:11px;color:var(--muted)">${buckets.length} buckets · ${trend} trend</div>`;
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

// ── Momentum Rank ─────────────────────────────────────────────────────────────
async function renderMomentumRank() {
  const data = await apiFetch('/momentum-rank');
  const el   = document.getElementById('momentum-rank-content');
  const badge = document.getElementById('momentum-rank-badge');
  if (!el) return;
  if (!data) { setErr('momentum-rank-content'); return; }

  const ranked = data.ranked || [];
  if (ranked.length === 0) {
    el.innerHTML = '<div style="color:var(--muted);font-size:11px;">No data</div>';
    return;
  }

  const top = ranked[0];
  if (badge) {
    badge.textContent = top.direction === 'bull' ? 'BULL' : top.direction === 'bear' ? 'BEAR' : 'FLAT';
    badge.className = `card-badge ${top.direction === 'bull' ? 'badge-green' : top.direction === 'bear' ? 'badge-red' : 'badge-blue'}`;
    badge.style.display = '';
  }

  function fmtPct(v) {
    if (v == null) return '<span style="color:var(--muted)">—</span>';
    const col = v > 0 ? 'var(--green)' : v < 0 ? 'var(--red)' : 'var(--muted)';
    return `<span style="color:${col}">${v > 0 ? '+' : ''}${v.toFixed(2)}%</span>`;
  }

  const rows = ranked.map(r => `<tr>
    <td style="padding:2px 6px 2px 0;font-size:11px;font-weight:600">${r.symbol.replace('USDT','')}</td>
    <td style="padding:2px 6px;font-size:10px;text-align:right">${fmtPct(r.pct_5m)}</td>
    <td style="padding:2px 6px;font-size:10px;text-align:right">${fmtPct(r.pct_15m)}</td>
    <td style="padding:2px 0;font-size:10px;text-align:right">${fmtPct(r.pct_1h)}</td>
  </tr>`).join('');

  el.innerHTML = `<table style="width:100%;border-collapse:collapse">
    <thead><tr style="color:var(--muted);font-size:9px;text-transform:uppercase">
      <th style="text-align:left;padding:2px 6px 4px 0;font-weight:400">sym</th>
      <th style="text-align:right;padding:2px 6px 4px;font-weight:400">5m</th>
      <th style="text-align:right;padding:2px 6px 4px;font-weight:400">15m</th>
      <th style="text-align:right;padding:2px 0 4px;font-weight:400">1h</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

// ── WS Stats (header display) ─────────────────────────────────────────────────
async function renderWsStats() {
  // Updates header connection indicator if present
  const el = document.getElementById('ws-stats-content');
  if (!el) return;
  const data = await apiFetch('/ws-stats');
  if (!data) { setErr('ws-stats-content'); return; }
  el.innerHTML = `<span style="font-size:11px;color:var(--muted)">${data.connections} conn · ${data.messages_per_sec}/s</span>`;
}

// ── CVD Divergence ────────────────────────────────────────────────────────────
async function renderCvdDivergence() {
  const sym   = encodeURIComponent(activeSymbol);
  const el    = document.getElementById('cvd-divergence-content');
  const badge = document.getElementById('cvd-divergence-badge');
  if (!el) return;
  const data = await apiFetch(`/cvd-divergence?symbol=${sym}`);
  if (!data) { setErr('cvd-divergence-content'); return; }
  const sig = (data[activeSymbol] || data).signal || 'none';
  const sev = (data[activeSymbol] || data).severity || 0;
  if (badge) {
    badge.textContent = sev > 0 ? sig.toUpperCase() : 'OK';
    badge.className = `card-badge ${sev >= 2 ? 'badge-red' : sev === 1 ? 'badge-yellow' : 'badge-green'}`;
    badge.style.display = '';
  }
  const desc = (data[activeSymbol] || data).description || '—';
  el.innerHTML = `<div style="font-size:11px;color:var(--fg)">${desc}</div>`;
}

// ── Squeeze Setup W11 ─────────────────────────────────────────────────────────
async function renderSqueezeSetupW11() {
  const sym   = encodeURIComponent(activeSymbol);
  const el    = document.getElementById('squeeze-setup-w11-content');
  const badge = document.getElementById('squeeze-setup-w11-badge');
  if (!el) return;
  const data = await apiFetch(`/squeeze-setup?symbol=${sym}`);
  if (!data) { setErr('squeeze-setup-w11-content'); return; }
  const score = data.score ?? 0;
  const signal = data.signal || 'none';
  if (badge) {
    badge.textContent = signal.toUpperCase();
    badge.className = `card-badge ${score >= 3 ? 'badge-red' : score >= 1 ? 'badge-yellow' : 'badge-blue'}`;
    badge.style.display = '';
  }
  el.innerHTML = `<div style="font-size:11px;"><span style="color:var(--muted)">score: </span><span style="font-weight:600">${score}</span> &nbsp; ${data.description || ''}</div>`;
}

// ── Flow Imbalance ────────────────────────────────────────────────────────────
async function renderFlowImbalance() {
  const sym   = encodeURIComponent(activeSymbol);
  const el    = document.getElementById('flow-imbalance-content');
  const badge = document.getElementById('flow-imbalance-badge');
  if (!el) return;
  const data = await apiFetch(`/flow-imbalance?symbol=${sym}`);
  if (!data) { setErr('flow-imbalance-content'); return; }
  const imb = data.imbalance ?? 0;
  const dir = imb > 0.1 ? 'BUY' : imb < -0.1 ? 'SELL' : 'FLAT';
  if (badge) {
    badge.textContent = dir;
    badge.className = `card-badge ${dir === 'BUY' ? 'badge-green' : dir === 'SELL' ? 'badge-red' : 'badge-blue'}`;
    badge.style.display = '';
  }
  const col = imb > 0 ? 'var(--green)' : imb < 0 ? 'var(--red)' : 'var(--muted)';
  el.innerHTML = `<div style="font-size:11px;"><span style="color:var(--muted)">imbalance: </span><span style="color:${col};font-weight:600">${(imb * 100).toFixed(1)}%</span></div>`;
}

// ── Volatility Regime ─────────────────────────────────────────────────────────
async function renderVolatilityRegime() {
  const sym   = encodeURIComponent(activeSymbol);
  const el    = document.getElementById('volatility-regime-content');
  const badge = document.getElementById('volatility-regime-badge');
  if (!el) return;
  const data = await apiFetch(`/volatility-regime?symbol=${sym}`);
  if (!data) { setErr('volatility-regime-content'); return; }
  const regime = data.regime || 'unknown';
  if (badge) {
    badge.textContent = regime.toUpperCase();
    badge.className = `card-badge ${regime === 'high' ? 'badge-red' : regime === 'medium' ? 'badge-yellow' : 'badge-green'}`;
    badge.style.display = '';
  }
  const pct = data.percentile != null ? data.percentile.toFixed(1) + '%' : '—';
  el.innerHTML = `<div style="font-size:11px;"><span style="color:var(--muted)">percentile: </span><span style="font-weight:600">${pct}</span></div>`;
}

// ── Price Velocity ────────────────────────────────────────────────────────────
async function renderPriceVelocity() {
  const sym   = encodeURIComponent(activeSymbol);
  const el    = document.getElementById('price-velocity-content');
  const badge = document.getElementById('price-velocity-badge');
  if (!el) return;
  const data = await apiFetch(`/price-velocity?symbol=${sym}`);
  if (!data) { setErr('price-velocity-content'); return; }
  const symData = data[activeSymbol] || Object.values(data).find(v => v && typeof v === 'object' && 'direction' in v) || data;
  const dir = symData.direction || 'flat';
  const score = symData.score ?? 0;
  if (badge) {
    badge.textContent = dir;
    badge.className = `card-badge ${dir === 'up' ? 'badge-green' : dir === 'down' ? 'badge-red' : 'badge-blue'}`;
    badge.style.display = '';
  }
  const col = score > 0 ? 'var(--green)' : score < 0 ? 'var(--red)' : 'var(--muted)';
  el.innerHTML = `<div style="font-size:11px;"><span style="color:var(--muted)">score: </span><span style="color:${col};font-weight:600">${score > 0 ? '+' : ''}${score}</span></div>`;
}

// ── Cross-Asset Correlation ───────────────────────────────────────────────────
async function renderCrossAssetCorr() {
  const sym   = encodeURIComponent(activeSymbol);
  const el    = document.getElementById('cross-asset-corr-content');
  const badge = document.getElementById('cross-asset-corr-badge');
  if (!el) return;
  const data = await apiFetch(`/cross-asset-corr?symbol=${sym}`);
  if (!data) { setErr('cross-asset-corr-content'); return; }

  const benchmarks = data.benchmarks || ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT'];
  const symbols    = data.symbols    || [];
  const matrix     = data.matrix     || {};
  const sp         = data.strongest_pair || {};

  if (badge && sp.corr != null) {
    const r = sp.corr;
    const cls = Math.abs(r) >= 0.7 ? 'badge-green' : Math.abs(r) >= 0.4 ? 'badge-yellow' : 'badge-blue';
    badge.textContent = `${sp.symbol?.split('USDT')[0] || ''}↔${sp.benchmark?.split('USDT')[0] || ''} ${r > 0 ? '+' : ''}${r.toFixed(2)}`;
    badge.className = `card-badge ${cls}`;
    badge.style.display = '';
  }

  // Colour for a correlation value
  const corrColor = r => {
    if (r == null) return 'var(--muted)';
    if (r >= 0.7)  return 'var(--green)';
    if (r >= 0.4)  return '#86efac';
    if (r >= 0.1)  return 'var(--blue)';
    if (r >= -0.1) return 'var(--muted)';
    if (r >= -0.4) return '#fca5a5';
    return 'var(--red)';
  };

  const shortName = s => s.replace('USDT', '');

  // Build matrix table (rows = our symbols, cols = benchmarks)
  const header = `<tr><th style="width:90px"></th>${benchmarks.map(b =>
    `<th style="font-size:9px;color:var(--muted);text-align:center">${shortName(b)}</th>`
  ).join('')}</tr>`;

  const rows = symbols.map(sym => {
    const row = matrix[sym] || {};
    const cells = benchmarks.map(b => {
      const v = row[b];
      const col = corrColor(v);
      const txt = v != null ? (v > 0 ? '+' : '') + v.toFixed(2) : '—';
      return `<td style="text-align:center;color:${col};font-weight:600;font-size:10px">${txt}</td>`;
    }).join('');
    return `<tr><td style="font-size:9px;color:var(--muted);padding-right:6px">${shortName(sym)}</td>${cells}</tr>`;
  }).join('');

  const desc = data.description || '';

  el.innerHTML = `
    <table style="border-collapse:collapse;width:100%;margin-bottom:4px">
      <thead>${header}</thead>
      <tbody>${rows}</tbody>
    </table>
    <div style="font-size:10px;color:var(--muted)">${desc}</div>`;
}

// ── Main Refresh Loop ─────────────────────────────────────────────────────────
async function refresh() {
  if (!activeSymbol) return;
  const safe = fn => fn().catch(e => console.warn('[refresh]', fn.name, e.message));

  try {
    // Batch 1: core price charts
    await Promise.all([safe(renderPriceChart), safe(renderOiChart), safe(renderCvdChart)]);
    await delay(200);

    // Batch 2: header stats
    await Promise.all([safe(renderFunding), safe(renderFundingMomentum), safe(renderSpread), safe(renderWsStats)]);
    await delay(200);

    // Batch 3: trade tape + imbalance
    await Promise.all([safe(renderTradeTape), safe(renderVolumeImbalance), safe(renderPhase)]);
    await delay(200);

    // Batch 4: OI analysis
    await Promise.all([safe(renderOiDivergence), safe(renderMicrostructure), safe(renderWhaleClustering)]);
    await delay(200);

    // Batch 5: price deviation metrics
    await Promise.all([safe(renderVwapDeviation), safe(renderOiWeightedPrice), safe(renderRealizedVolBands)]);
    await delay(200);

    // Batch 6: regime & momentum
    await Promise.all([safe(renderMarketRegime), safe(renderMomentum), safe(renderMomentumRank), safe(renderRegimeTimeline)]);
    await delay(200);

    // Batch 7: correlations
    await Promise.all([safe(renderCorrelations), safe(renderCorrHeatmap), safe(renderVolumeProfile)]);
    await delay(200);

    // Batch 8: aggressor metrics
    await Promise.all([safe(renderAggressorRatio), safe(renderVpin), safe(renderAdaptiveVolumeProfile)]);
    await delay(200);

    // Batch 9: tape analysis
    await Promise.all([safe(renderTapeSpeed), safe(renderAggressorStreak), safe(renderObWalls)]);
    await delay(200);

    // Batch 10: movers, heatmap, net taker
    await Promise.all([safe(renderTopMovers), safe(renderLiqHeatmap), safe(renderNetTakerDelta)]);
    await delay(200);

    // Batch 11: new signal cards
    await Promise.all([safe(renderCvdMomentum), safe(renderDeltaDivergence), safe(renderFundingExtreme)]);
    await delay(200);

    // Batch 12: cascade & large trades
    await Promise.all([safe(renderLiqCascade), safe(renderLargeTrades)]);
    await delay(200);

    // Batch 13: alerts, oi-delta, squeeze, volume spike, trade count rate
    await Promise.all([
      safe(renderAlerts),
      safe(renderOiDelta),
      safe(renderSqueezeSetup),
      safe(renderVolumeSpikeCard),
      safe(renderTradeCountRate),
    ]);
    await delay(200);

    // Batch 14: wave 11 cards
    await Promise.all([
      safe(renderCvdDivergence),
      safe(renderSqueezeSetupW11),
      safe(renderFlowImbalance),
      safe(renderVolatilityRegime),
      safe(renderPriceVelocity),
    ]);

    await delay(200);
    // Batch 15: cross-asset correlation
    await Promise.all([safe(renderCrossAssetCorr)]);
    // Batch 16: social sentiment
    await Promise.all([safe(renderSocialSentiment)]);

    // Batch 17: rv-iv card
        await delay(200);

    // Batch 18: session volume profile
        // Batch 19: OFT
        // Batch 20: momentum divergence
        // Batch 21: spread analysis
        // Batch 22: options skew
        // Batch 23: miner reserve (global BTC signal, no symbol)
    await Promise.all([safe(renderMinerReserve)]);
    // Batch 24: macro liquidity indicator
    await Promise.all([safe(renderMacroLiquidity)]);
    // Batch 24: token velocity + NVT
    await Promise.all([safe(renderTokenVelocityNvt)]);
    // Batch 16: derivatives heatmap
        // Batch 25: holder distribution card
        // Batch 26: cross-chain arb monitor
    await Promise.all([safe(refreshCrossChainArb)]);
    // Batch 27: volatility regime detector
    await Promise.all([safe(refreshVolatilityRegimeDetector)]);
    // Batch 28: smart money index
    await Promise.all([safe(renderSmartMoneyIndex)]);
    await delay(200);
    // Batch 29: order flow toxicity (VPIN)
    await Promise.all([safe(renderOrderFlowToxicity)]);
    // Batch 30: liquidation cascade detector
    await Promise.all([safe(renderLiqCascadeDetector)]);
  } finally {
    _refreshRunning = false;
  }
}

// ── Social Sentiment ──────────────────────────────────────────────────────────
async function renderSocialSentiment() {
  const el    = document.getElementById('social-sentiment-content');
  const badge = document.getElementById('social-sentiment-badge');
  if (!el) return;
  const data = await apiFetch('/social-sentiment');
  if (!data) { setErr('social-sentiment-content'); return; }

  const sent   = data.sentiment    || {};
  const vol    = data.social_volume || {};
  const kw     = data.keywords      || {};
  const hist   = data.history       || [];
  const label  = sent.label         || 'neutral';
  const score  = sent.score         ?? 50;
  const dir    = sent.direction     || 'stable';
  const mom    = sent.momentum      ?? 0;
  const zscore = data.zscore        ?? 0;

  const sigCls = label === 'very_bullish' ? 'badge-green'
               : label === 'bullish'      ? 'badge-green'
               : label === 'bearish'      ? 'badge-red'
               : label === 'very_bearish' ? 'badge-red'
               : 'badge-blue';
  const sigLabel = label.replace('_', ' ').toUpperCase();
  if (badge) {
    badge.textContent = sigLabel;
    badge.className   = `card-badge ${sigCls}`;
    badge.style.display = '';
  }

  const scoreCol = score >= 60 ? 'var(--green)' : score <= 40 ? 'var(--red)' : 'var(--muted)';
  const dirCol   = dir === 'rising' ? 'var(--green)' : dir === 'falling' ? 'var(--red)' : 'var(--muted)';
  const domCol   = kw.dominant === 'bullish' ? 'var(--green)' : kw.dominant === 'bearish' ? 'var(--red)' : 'var(--muted)';

  const sparkbars = hist.slice(-10).map(h => {
    const col = (h.score || 50) >= 55 ? 'var(--green)' : (h.score || 50) <= 40 ? 'var(--red)' : 'var(--muted)';
    return `<span style="display:inline-block;width:5px;height:10px;background:${col};margin-right:1px;opacity:0.7"></span>`;
  }).join('');

  const kwBull = (kw.top_bullish || []).join(', ') || '—';
  const kwBear = (kw.top_bearish || []).join(', ') || '—';

  el.innerHTML = `
    <div style="font-size:10px;color:var(--muted);margin-bottom:4px;display:flex;gap:10px;flex-wrap:wrap">
      <span>score: <b style="color:${scoreCol}">${score.toFixed(1)}/100</b></span>
      <span>trend: <b style="color:${dirCol}">${dir}</b></span>
      <span>mom: <b style="color:${mom>=0?'var(--green)':'var(--red)'}">${mom>=0?'+':''}${mom.toFixed(1)}</b></span>
      <span>z: <b style="color:${zscore>1?'var(--green)':zscore<-1?'var(--red)':'var(--muted)'}">${zscore.toFixed(2)}</b></span>
    </div>
    <div style="font-size:10px;color:var(--muted);margin-bottom:4px;display:flex;gap:10px;flex-wrap:wrap">
      <span>buzz: <b style="color:var(--fg)">${vol.buzz || '—'}</b></span>
      <span>vol: <b style="color:var(--fg)">${(vol.volume_proxy||0).toFixed(1)}</b></span>
      <span>reddit/h: <b style="color:var(--fg)">${vol.reddit_posts_per_hour||0}p ${vol.reddit_comments_per_hour||0}c</b></span>
    </div>
    <div style="font-size:10px;color:var(--muted);margin-bottom:4px">
      keywords: <span style="color:var(--green)">▲${kw.bullish_count||0}</span>
      <span style="color:var(--red)"> ▼${kw.bearish_count||0}</span>
      <span style="color:var(--muted)"> ●${kw.neutral_count||0}</span>
      dominant: <b style="color:${domCol}">${kw.dominant||'neutral'}</b>
    </div>
    <div style="font-size:9px;color:var(--muted);margin-bottom:4px">
      bull: <span style="color:var(--green)">${kwBull}</span>
      &nbsp; bear: <span style="color:var(--red)">${kwBear}</span>
    </div>
    <div style="margin-top:4px">${sparkbars}</div>
    ${data.description ? `<div style="font-size:10px;color:var(--muted);margin-top:4px">${data.description}</div>` : ''}`;
}


// ── Network Health Score ──────────────────────────────────────────────────────
// ── Miner Reserve Indicator ───────────────────────────────────────────────────
async function renderMinerReserve() {
  const data  = await apiFetch('/miner-reserve');
  const el    = document.getElementById('miner-reserve-content');
  const badge = document.getElementById('miner-reserve-badge');
  if (!data || !el) return;

  const signal = data.signal ?? 'neutral';
  const trend  = data.reserve_trend ?? 'stable';
  const spi    = data.sell_pressure_index ?? 0;
  const spiPct = data.spi_percentile ?? 50;

  const sigCol = signal === 'bullish' ? 'var(--green)'
    : signal === 'bearish' ? 'var(--red)' : 'var(--muted)';

  if (badge) {
    badge.textContent = signal.toUpperCase();
    badge.style.display = 'inline-block';
    badge.style.color = sigCol;
  }

  const fmtUsd = v => {
    const av = Math.abs(v || 0);
    if (av >= 1e9) return '$' + (av / 1e9).toFixed(2) + 'B';
    if (av >= 1e6) return '$' + (av / 1e6).toFixed(0) + 'M';
    return '$' + av.toFixed(0);
  };

  const trendCol  = trend === 'accumulating' ? 'var(--green)'
    : trend === 'depleting' ? 'var(--red)' : 'var(--muted)';
  const trendIcon = trend === 'accumulating' ? '↑' : trend === 'depleting' ? '↓' : '→';

  const hrChange = data.hash_rate_change_30d_pct ?? 0;
  const hrCol    = hrChange > 0 ? 'var(--green)' : hrChange < 0 ? 'var(--red)' : 'var(--muted)';

  // SPI gauge bar
  const spiBar = `
    <div style="display:flex;align-items:center;gap:6px;margin-bottom:4px">
      <span style="font-size:9px;color:var(--muted);width:20px">SPI</span>
      <div style="flex:1;height:5px;background:var(--border);border-radius:3px">
        <div style="width:${Math.min(spi, 100).toFixed(0)}%;height:100%;background:${spi > 25 ? 'var(--red)' : spi > 10 ? '#f59e0b' : 'var(--green)'};border-radius:3px"></div>
      </div>
      <span style="font-size:9px;color:var(--muted);width:36px;text-align:right">${spi.toFixed(1)}% <span style="font-size:8px">(${spiPct.toFixed(0)}p)</span></span>
    </div>`;

  // 30-day history sparkline
  const hist = (data.history || []).slice(-30);
  let sparkline = '';
  if (hist.length >= 2) {
    const spiVals = hist.map(h => h.spi ?? 0);
    const sMax = Math.max(...spiVals, 0.01);
    const W = 120, H = 22;
    const pts = spiVals.map((s, i) => {
      const x = (i / (spiVals.length - 1)) * W;
      const y = H - (s / sMax) * H;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(' ');
    sparkline = `<div style="font-size:9px;color:var(--muted);margin-bottom:2px">30d SPI</div>
    <svg width="${W}" height="${H}" style="display:block;margin-bottom:4px">
      <polyline points="${pts}" fill="none" stroke="${sigCol}" stroke-width="1.5"/>
    </svg>`;
  }

  const depDays = data.depletion_rate_days;
  const depStr  = depDays >= 9999 ? '∞' : depDays.toFixed(0) + 'd';

  el.innerHTML = `
    <div style="font-size:10px;color:var(--muted);display:flex;flex-wrap:wrap;gap:4px 12px;margin-bottom:4px">
      <span>reserve: <b style="color:var(--fg)">${fmtUsd(data.miner_reserve_usd)}</b></span>
      <span>outflow: <b style="color:var(--red)">${fmtUsd(data.daily_outflow_usd)}/d</b></span>
    </div>
    <div style="font-size:10px;color:var(--muted);display:flex;flex-wrap:wrap;gap:4px 12px;margin-bottom:4px">
      <span>trend: <b style="color:${trendCol}">${trendIcon} ${trend}</b></span>
      <span>depletion: <b style="color:var(--fg)">${depStr}</b></span>
      <span>z: <b style="color:var(--fg)">${(data.outflow_zscore ?? 0).toFixed(2)}</b></span>
    </div>
    <div style="font-size:10px;color:var(--muted);margin-bottom:4px">
      hashrate: <b style="color:${hrCol}">${(data.hash_rate ?? 0).toFixed(1)} EH/s (${hrChange >= 0 ? '+' : ''}${hrChange.toFixed(1)}% 30d)</b>
    </div>
    ${spiBar}
    ${sparkline}
    ${data.description ? `<div style="font-size:10px;color:var(--muted)">${data.description}</div>` : ''}`;
}

// ── Macro Liquidity Indicator ────────────────────────────────────────────
async function renderMacroLiquidity() {
  const data  = await apiFetch('/macro-liquidity-indicator');
  const el    = document.getElementById('macro-liquidity-content');
  const badge = document.getElementById('macro-liquidity-badge');
  if (!data || !el) return;

  const regime = data.regime ?? {};
  const m2     = data.m2 ?? {};
  const fed    = data.fed_balance_sheet ?? {};
  const dxy    = data.usd_index ?? {};

  const label  = regime.label ?? 'neutral';
  const score  = regime.score ?? 50;
  const trend  = regime.trend ?? 'stable';

  const lblCol = label === 'risk_on' ? 'var(--green)'
    : label === 'risk_off' ? 'var(--red)' : 'var(--muted)';
  const trendIcon = trend === 'expanding' ? '↑' : trend === 'contracting' ? '↓' : '→';

  if (badge) {
    badge.textContent = label.replace('_', '-').toUpperCase();
    badge.style.display = 'inline-block';
    badge.style.color = lblCol;
  }

  const fmtT  = v => { const a = Math.abs(v||0); return a >= 1e12 ? '$' + (a/1e12).toFixed(1) + 'T' : a >= 1e9 ? '$' + (a/1e9).toFixed(0) + 'B' : '$' + a.toFixed(0); };
  const fmtP  = v => (v >= 0 ? '+' : '') + (v ?? 0).toFixed(2) + '%';
  const fmtS  = v => (v ?? 0).toFixed(1);

  // Regime score gauge bar
  const barW   = Math.min(score, 100).toFixed(0);
  const barCol = score >= 60 ? 'var(--green)' : score >= 40 ? '#f59e0b' : 'var(--red)';
  const gauge  = `
    <div style="display:flex;align-items:center;gap:6px;margin:4px 0">
      <span style="font-size:9px;color:var(--muted);width:36px">SCORE</span>
      <div style="flex:1;height:6px;background:var(--border);border-radius:3px">
        <div style="width:${barW}%;height:100%;background:${barCol};border-radius:3px"></div>
      </div>
      <span style="font-size:9px;color:${barCol};width:32px;text-align:right">${fmtS(score)}</span>
    </div>`;

  const fedDelta = fed.delta_30d_usd ?? 0;
  const fedCol   = fedDelta > 0 ? 'var(--green)' : fedDelta < 0 ? 'var(--red)' : 'var(--muted)';
  const dxyCol   = (dxy.change_30d_pct ?? 0) < 0 ? 'var(--green)' : 'var(--red)';  // USD weak = bullish
  const divCol   = (dxy.btc_divergence ?? 0) > 0 ? 'var(--green)' : 'var(--red)';

  el.innerHTML = `
    <div style="display:flex;gap:12px;margin-bottom:4px;align-items:flex-start">
      <div>
        <div style="font-size:9px;color:var(--muted)">REGIME</div>
        <div style="font-size:15px;font-weight:700;color:${lblCol}">${label.replace('_','-').toUpperCase()}</div>
        <div style="font-size:9px;color:var(--muted)">${trendIcon} ${trend} vs 90d MA ${fmtS(regime.ma_90d)}</div>
      </div>
      <div>
        <div style="font-size:9px;color:var(--muted)">M2 YOY</div>
        <div style="font-size:13px;font-weight:600">${fmtP(m2.growth_rate_yoy_pct)}</div>
      </div>
      <div>
        <div style="font-size:9px;color:var(--muted)">DXY 30D</div>
        <div style="font-size:13px;font-weight:600;color:${dxyCol}">${fmtP(dxy.change_30d_pct)}</div>
      </div>
      <div>
        <div style="font-size:9px;color:var(--muted)">BTC DIV</div>
        <div style="font-size:13px;font-weight:600;color:${divCol}">${fmtP(dxy.btc_divergence)}</div>
      </div>
    </div>
    ${gauge}
    <div style="display:flex;gap:8px;margin-top:4px;font-size:9px">
      <span style="color:var(--muted)">FED:</span>
      <span style="color:${fedCol}">${fmtT(Math.abs(fedDelta))} ${fedDelta >= 0 ? 'QE' : 'QT'}</span>
      <span style="color:var(--muted);margin-left:6px">FED TOTAL:</span>
      <span>${fmtT(fed.current_usd)}</span>
      <span style="color:var(--muted);margin-left:6px">M2:</span>
      <span>${fmtT(m2.current_proxy_usd)}</span>
    </div>
    <div style="font-size:9px;color:var(--muted);margin-top:4px">${data.description ?? ''}</div>
  `;
}

// ── Layer 2 Metrics ────────────────────────────────────────────────────────────────
async function renderLayer2Metrics() {
  const data  = await apiFetch('/layer2-metrics');
  const el    = document.getElementById('layer2-metrics-content');
  const badge = document.getElementById('layer2-metrics-badge');
  if (!el) return;
  const mom  = data.momentum?.label ?? 'neutral';
  const score = (data.momentum?.score ?? 0).toFixed(1);
  const momClass = { strong_growth: 'badge-green', growing: 'badge-green', neutral: 'badge-gray', declining: 'badge-red' };
  if (badge) { badge.textContent = mom.replace('_',' ').toUpperCase(); badge.className = 'card-badge ' + (momClass[mom]??'badge-gray'); badge.style.display = ''; }
  const totalTvl = data.aggregate?.total_tvl_usd ?? 0;
  const ch24 = (data.aggregate?.total_tvl_change_24h_pct ?? 0);
  const ch24Str = (ch24>=0?'+':'') + ch24.toFixed(2) + '%';
  const ch24Col = ch24 >= 0 ? 'var(--green)' : 'var(--red)';
  const gasSav = (data.aggregate?.avg_gas_savings_pct ?? 0).toFixed(1);
  const topChain = data.aggregate?.top_chain ?? 'Arbitrum';
  const leader = data.momentum?.leader ?? topChain;
  const fmtB = v => v>=1e9?'$'+(v/1e9).toFixed(1)+'B':v>=1e6?'$'+(v/1e6).toFixed(0)+'M':'$'+v.toFixed(0);
  const CHAIN_ORDER = ['Arbitrum','Optimism','Base','Polygon','zkSync'];
  const CHAIN_COLS = {Arbitrum:'#1a91ff',Optimism:'#ff0420',Base:'#0052ff',Polygon:'#8247e5',zkSync:'#4e529a'};
  const chains = data.chains ?? {};
  const maxTvl = Math.max(...CHAIN_ORDER.map(c=>(chains[c]?.tvl_usd??0)),1);
  const chainRows = CHAIN_ORDER.map(name=>{
    const c=chains[name]??{}; const tvl=c.tvl_usd??0; const w=(tvl/maxTvl*100).toFixed(0);
    const ch=c.tvl_change_24h_pct??0; const chStr=(ch>=0?'+':'')+ch.toFixed(1)+'%';
    const chCol=ch>=0?'var(--green)':'var(--red)'; const dir=c.bridge_direction??'neutral';
    const dirIcon=dir==='inflow'?'↓':dir==='outflow'?'↑':'→'; const col=CHAIN_COLS[name]??'var(--muted)';
    return '<div style="display:flex;align-items:center;gap:4px;margin-bottom:2px;font-size:9px">'+
      '<span style="width:52px;color:var(--muted)">'+name+'</span>'+
      '<div style="flex:1;height:5px;background:var(--border);border-radius:2px">'+
      '<div style="width:'+w+'%;height:100%;background:'+col+';border-radius:2px"></div></div>'+
      '<span style="color:var(--fg);min-width:32px;text-align:right">'+fmtB(tvl)+'</span>'+
      '<span style="color:'+chCol+';min-width:36px;text-align:right">'+chStr+'</span>'+
      '<span style="color:'+(dir==='inflow'?'var(--green)':dir==='outflow'?'var(--red)':'var(--muted)')+'">'+dirIcon+'</span></div>';
  }).join('');
  const sp=data.history_7d??[];
  let sparkSvg='';
  if(sp.length>=2){const vals=sp.map(p=>p.total_tvl_usd??0);const mn=Math.min(...vals)*0.998;const mx=Math.max(...vals)*1.002;const W=200,H=22;const px=i=>(i/(sp.length-1))*W;const py=v=>H-((v-mn)/(mx-mn||1))*H;const path=vals.map((v,i)=>(i===0?'M':'L')+px(i).toFixed(1)+','+py(v).toFixed(1)).join(' ');sparkSvg='<svg width="'+W+'" height="'+H+'" style="display:block;margin-bottom:4px"><path d="'+path+'" stroke="var(--green)" stroke-width="1.2" fill="none"/></svg>';}
  el.innerHTML='<div style="font-size:10px;color:var(--muted);display:flex;flex-wrap:wrap;gap:4px 12px;margin-bottom:4px">'+
    '<span>TVL: <b style="color:var(--fg)">'+fmtB(totalTvl)+'</b> <span style="color:'+ch24Col+'">'+ch24Str+' 24h</span></span>'+
    '<span>gas saved: <b style="color:var(--green)">'+gasSav+'%</b></span></div>'+
    '<div style="font-size:10px;color:var(--muted);margin-bottom:4px">'+chainRows+'</div>'+
    '<div style="font-size:10px;color:var(--muted);display:flex;gap:12px;margin-bottom:4px">'+
    '<span>top: <b style="color:var(--fg)">'+topChain+'</b></span>'+
    '<span>leader: <b style="color:var(--green)">'+leader+'</b></span>'+
    '<span>score: <b style="color:var(--fg)">'+score+'</b></span></div>'+
    sparkSvg+(data.description?'<div style="font-size:10px;color:var(--muted)">'+data.description+'</div>':'');
}

// ── Token Velocity + NVT ──────────────────────────────────────────────────────
async function renderTokenVelocityNvt() {
  const el    = document.getElementById('token-velocity-nvt-content');
  const badge = document.getElementById('token-velocity-nvt-badge');
  if (!el) return;
  const data = await apiFetch('/token-velocity-nvt');
  if (!data) { setErr('token-velocity-nvt-content'); return; }

  const vel    = data.velocity || {};
  const nvt    = data.nvt      || {};
  const hist   = data.history  || [];
  const label  = nvt.label     || 'neutral';
  const signal = nvt.signal    ?? 0;
  const ratio  = nvt.ratio     ?? 0;
  const zscore = nvt.zscore    ?? 0;
  const obThr  = nvt.overbought_threshold ?? 150;
  const osThr  = nvt.oversold_threshold   ?? 45;

  const labelCls = label === 'overbought' ? 'badge-red'
                 : label === 'oversold'   ? 'badge-green'
                 : label === 'fair_value' ? 'badge-green'
                 : 'badge-blue';
  const labelTxt = label.replace('_', ' ').toUpperCase();
  if (badge) {
    badge.textContent = labelTxt;
    badge.className   = `card-badge ${labelCls}`;
    badge.style.display = '';
  }

  const gaugeMax  = 200;
  const gaugePct  = Math.min(signal / gaugeMax * 100, 100).toFixed(1);
  const gaugeCol  = signal >= obThr ? 'var(--red)'
                  : signal <= osThr ? 'var(--green)'
                  : signal <= 90    ? 'var(--green)'
                  : 'var(--blue)';
  const obPct  = (obThr / gaugeMax * 100).toFixed(1);
  const osPct  = (osThr / gaugeMax * 100).toFixed(1);

  const trendCol = vel.trend === 'accelerating' ? 'var(--green)'
                 : vel.trend === 'decelerating' ? 'var(--red)'
                 : 'var(--muted)';

  const sparkbars = hist.slice(-14).map(h => {
    const v   = h.nvt_signal ?? 0;
    const col = v >= obThr ? 'var(--red)' : v <= osThr ? 'var(--green)' : 'var(--blue)';
    return `<span style="display:inline-block;width:5px;height:10px;background:${col};margin-right:1px;opacity:0.7"></span>`;
  }).join('');

  const fmtB = v => {
    if (v >= 1e12) return (v / 1e12).toFixed(2) + 'T';
    if (v >= 1e9)  return (v / 1e9).toFixed(1)  + 'B';
    return v.toFixed(0);
  };

  el.innerHTML = `
    <div style="font-size:10px;color:var(--muted);margin-bottom:6px">
      <div style="display:flex;align-items:center;gap:6px;margin-bottom:3px">
        <span style="font-size:9px;color:var(--muted);width:60px">NVT signal</span>
        <div style="flex:1;background:var(--border);height:8px;border-radius:3px;position:relative">
          <div style="width:${gaugePct}%;background:${gaugeCol};height:100%;border-radius:3px"></div>
          <div style="position:absolute;top:-2px;left:${obPct}%;width:1px;height:12px;background:var(--red);opacity:0.6"></div>
          <div style="position:absolute;top:-2px;left:${osPct}%;width:1px;height:12px;background:var(--green);opacity:0.6"></div>
        </div>
        <b style="color:${gaugeCol};font-size:10px;min-width:36px;text-align:right">${signal.toFixed(1)}</b>
      </div>
    </div>
    <div style="font-size:10px;color:var(--muted);margin-bottom:4px;display:flex;gap:10px;flex-wrap:wrap">
      <span>NVT ratio: <b style="color:var(--fg)">${ratio.toFixed(1)}</b></span>
      <span>z: <b style="color:${zscore>1?'var(--green)':zscore<-1?'var(--red)':'var(--muted)'}">${zscore.toFixed(2)}</b></span>
    </div>
    <div style="font-size:10px;color:var(--muted);margin-bottom:4px;display:flex;gap:10px;flex-wrap:wrap">
      <span>velocity: <b style="color:var(--fg)">${(vel.current||0).toFixed(4)}</b></span>
      <span>trend: <b style="color:${trendCol}">${vel.trend||'—'}</b></span>
      <span>7d: <b style="color:var(--fg)">${(vel.velocity_7d||0).toFixed(4)}</b></span>
      <span>30d: <b style="color:var(--fg)">${(vel.velocity_30d||0).toFixed(4)}</b></span>
    </div>
    <div style="font-size:10px;color:var(--muted);margin-bottom:4px">
      mktcap: <b style="color:var(--fg)">$${fmtB(data.market_cap_usd||0)}</b>
      &nbsp; tx/24h: <b style="color:var(--fg)">$${fmtB(data.tx_volume_24h_usd||0)}</b>
    </div>
    <div style="margin-top:4px">${sparkbars}</div>
    ${data.description ? `<div style="font-size:10px;color:var(--muted);margin-top:4px">${data.description}</div>` : ''}`;
}

// ── DEX vs CEX Volume Divergence ──────────────────────────────────────────────
async function refreshDexVsCexFlow() {
  const el = document.getElementById('dex-vs-cex-content');
  if (!el) return;
  const sym = activeSymbol ? `?symbol=${activeSymbol}` : '';
  const data = await apiFetch(`/api/dex-vs-cex-flow${sym}`);
  if (!data) { el.textContent = 'Unavailable'; return; }

  const badge = document.getElementById('dex-vs-cex-badge');
  const sig = data.discovery_signal ?? 'neutral';
  const sigColor = {
    strong_buy:  '#26a69a',
    watch:       '#ffa726',
    strong_sell: '#ef5350',
    neutral:     '#607d8b',
  }[sig] ?? '#607d8b';
  if (badge) {
    badge.textContent = sig.replace('_', ' ').toUpperCase();
    badge.style.background = sigColor;
    badge.style.display = 'inline-block';
  }

  const zscore  = data.divergence_zscore ?? 0;
  const zColor  = zscore > 1.5 ? '#26a69a' : zscore < -1.5 ? '#ef5350' : '#aaa';
  const domPct  = data.dex_dominance_pct ?? 0;
  const trend   = data.dominance_trend ?? 'stable';
  const trendArrow = trend === 'rising' ? '▲' : trend === 'falling' ? '▼' : '–';
  const trendColor = trend === 'rising' ? '#26a69a' : trend === 'falling' ? '#ef5350' : '#aaa';

  const fmtM = v => {
    if (v >= 1e9) return (v / 1e9).toFixed(2) + 'B';
    if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
    return v.toFixed(0);
  };

  const protocols = data.protocols ?? {};
  const pctMap    = data.protocol_breakdown_pct ?? {};
  const protoRows = Object.keys(protocols).map(k => {
    const pct = pctMap[k] ?? 0;
    return `<span style="color:#aaa">${k.replace('_', ' ')}: <b style="color:#e2e8f0">${fmtM(protocols[k])}</b> <span style="color:#555">(${pct}%)</span></span>`;
  }).join(' · ');

  const hist = data.dominance_history ?? [];
  const sparkMax = Math.max(...hist, 0.01);
  const sparkMin = Math.min(...hist, 0);
  const sparkBars = hist.map(v => {
    const h = Math.round(((v - sparkMin) / (sparkMax - sparkMin || 1)) * 20);
    const c = v > data.mean_dominance ? '#26a69a' : '#ef5350';
    return `<span style="display:inline-block;width:4px;height:${h + 2}px;background:${c};margin-right:1px;vertical-align:bottom"></span>`;
  }).join('');

  el.innerHTML = `
    <div style="display:flex;gap:12px;flex-wrap:wrap;font-size:10px;margin-bottom:5px">
      <span style="color:#aaa">DEX vol: <b style="color:#e2e8f0">$${fmtM(data.dex_volume_usd ?? 0)}</b></span>
      <span style="color:#aaa">CEX vol: <b style="color:#e2e8f0">$${fmtM(data.cex_volume_usd ?? 0)}</b></span>
      <span style="color:#aaa">DEX dom: <b style="color:#e2e8f0">${domPct.toFixed(1)}%</b> <span style="color:${trendColor}">${trendArrow}</span></span>
      <span style="color:#aaa">Z-score: <b style="color:${zColor}">${zscore.toFixed(2)}</b></span>
    </div>
    <div style="font-size:10px;color:#aaa;margin-bottom:4px">${protoRows}</div>
    <div style="margin:4px 0;line-height:22px">${sparkBars}</div>
    <div style="font-size:10px;color:#aaa">price discovery: <b style="color:${sigColor}">${(data.price_discovery ?? '').replace(/_/g, ' ')}</b></div>
    ${data.description ? `<div style="font-size:10px;color:#555;margin-top:3px">${data.description}</div>` : ''}
  `;
}

// ── Derivatives Heatmap ───────────────────────────────────────────────────────
// ── Holder Distribution ───────────────────────────────────────────────────
// ── Cross-Chain Bridge Monitor ────────────────────────────────────────────────
async function renderCrossChainBridge() {
  const el = document.getElementById('bridge-monitor-content');
  if (!el) return;
  const data = await fetchJSON('/api/cross-chain-bridge-monitor');
  if (!data) { el.textContent = 'Unavailable'; return; }
  const badge = document.getElementById('bridge-monitor-badge');
  const cong = data.congestion?.label ?? 'unknown';
  const congColor = { low: '#26a69a', moderate: '#ffa726', high: '#ef5350', severe: '#b71c1c' }[cong] ?? '#888';
  if (badge) {
    badge.textContent = cong.toUpperCase();
    badge.style.background = congColor;
    badge.style.display = 'inline-block';
  }
  const chains = data.chains ?? {};
  const chainNames = ['ETH', 'BSC', 'ARB', 'OP', 'BASE'];
  const flowColor = f => f > 5 ? '#26a69a' : f < -5 ? '#ef5350' : '#888';
  const chainRows = chainNames.map(c => {
    const d = chains[c] ?? {};
    return `<tr><td style="color:#aaa;width:45px">${c}</td><td>+${(d.inflow_24h??0).toFixed(0)}</td><td style="color:#666">-${(d.outflow_24h??0).toFixed(0)}</td><td style="color:${flowColor(d.net_flow??0)}">${(d.net_flow??0)>0?'+':''}${(d.net_flow??0).toFixed(1)}</td></tr>`;
  }).join('');
  const bridges = (data.bridges ?? []).slice(0,5);
  const bridgeList = bridges.map(b => `<span style="color:#aaa">${b.rank}.${b.name} <b>${(b.volume_24h??0).toFixed(0)}M</b></span>`).join(' · ');
  const dom = data.dominance ?? {};
  const anomalies = data.anomalies ?? [];
  const anomalyTxt = anomalies.length ? anomalies.map(a => `${a.chain} ${a.ratio}x avg`).join(', ') : 'none';
  el.innerHTML = `
    <table style="width:100%;border-collapse:collapse;margin-bottom:4px;font-size:10px">
      <tr style="color:#555"><th>Chain</th><th>In($M)</th><th>Out($M)</th><th>Net</th></tr>
      ${chainRows}
    </table>
    <div style="font-size:10px;color:#aaa;margin-bottom:3px">${bridgeList}</div>
    <div style="display:flex;gap:10px;font-size:10px;color:#aaa;flex-wrap:wrap">
      <span>Dom: <b>${dom.chain??'?'}</b> ${(dom.inflow_pct??0).toFixed(1)}%</span>
      <span>Vol: <b>${(data.total_volume_24h??0).toFixed(0)}M</b></span>
      <span>Anomaly: <b style="color:${anomalies.length?'#ef5350':'#26a69a'}">${anomalyTxt}</b></span>
    </div>
    <div style="margin-top:3px;color:#666;font-size:10px">${data.description ?? ''}</div>
  `;
}

// ── Validator Activity ────────────────────────────────────────────────────────
// ── NFT Market Pulse ──────────────────────────────────────────────────────
async function renderNftMarketPulse() {
  const data  = await apiFetch('/nft-market-pulse');
  const el    = document.getElementById('nft-market-pulse-content');
  const badge = document.getElementById('nft-market-pulse-badge');
  if (!data || !el) return;

  const idx   = data.bluechip_index ?? {};
  const mkt   = data.market ?? {};
  const trend = idx.trend ?? 'stable';
  const liq   = mkt.market_liquidity ?? 'cool';

  const trendCol = trend === 'rising' ? 'var(--green)'
    : trend === 'falling' ? 'var(--red)' : 'var(--muted)';
  const liqCol = liq === 'hot' ? 'var(--green)'
    : liq === 'warm' ? '#f59e0b'
    : liq === 'cool' ? 'var(--muted)' : 'var(--red)';

  if (badge) {
    badge.textContent = trend.toUpperCase();
    badge.style.display = 'inline-block';
    badge.style.color = trendCol;
  }

  const fmtEth = v => (v ?? 0).toFixed(1) + ' ETH';
  const fmtPct = v => (v >= 0 ? '+' : '') + (v ?? 0).toFixed(1) + '%';

  // Collection rows
  const colls = data.collections ?? {};
  const rows = Object.entries(colls).map(([name, c]) => {
    const chgCol = (c.floor_change_24h_pct ?? 0) >= 0 ? 'var(--green)' : 'var(--red)';
    const lCol   = c.liquidity === 'hot' ? 'var(--green)'
      : c.liquidity === 'warm' ? '#f59e0b'
      : c.liquidity === 'cold' ? 'var(--red)' : 'var(--muted)';
    return `<tr>
      <td style="color:var(--text);font-size:9px">${name.replace(' ', '\u00a0').substring(0,14)}</td>
      <td style="text-align:right;font-size:9px">${fmtEth(c.floor_eth)}</td>
      <td style="text-align:right;font-size:9px;color:${chgCol}">${fmtPct(c.floor_change_24h_pct)}</td>
      <td style="text-align:right;font-size:9px;color:${lCol}">${(c.liquidity ?? '').toUpperCase()}</td>
    </tr>`;
  }).join('');

  const corr = idx.btc_correlation ?? 0;
  const corrCol = corr > 0.5 ? 'var(--green)' : corr < -0.5 ? 'var(--red)' : 'var(--muted)';

  el.innerHTML = `
    <div style="display:flex;gap:12px;margin-bottom:6px">
      <div>
        <div style="font-size:9px;color:var(--muted)">BLUE-CHIP INDEX</div>
        <div style="font-size:16px;font-weight:700;color:${trendCol}">${(idx.value ?? 0).toFixed(1)}</div>
      </div>
      <div>
        <div style="font-size:9px;color:var(--muted)">BTC CORR</div>
        <div style="font-size:13px;font-weight:600;color:${corrCol}">${corr.toFixed(2)}</div>
      </div>
      <div>
        <div style="font-size:9px;color:var(--muted)">WASH %</div>
        <div style="font-size:13px;font-weight:600;color:var(--muted)">${(mkt.wash_trade_pct ?? 0).toFixed(1)}%</div>
      </div>
      <div>
        <div style="font-size:9px;color:var(--muted)">LIQUIDITY</div>
        <div style="font-size:13px;font-weight:600;color:${liqCol}">${liq.toUpperCase()}</div>
      </div>
    </div>
    <table style="width:100%;border-collapse:collapse">
      <thead><tr>
        <th style="font-size:8px;color:var(--muted);text-align:left;font-weight:500">COLLECTION</th>
        <th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">FLOOR</th>
        <th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">24H</th>
        <th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">LIQ</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
    <div style="font-size:9px;color:var(--muted);margin-top:4px">${data.description ?? ''}</div>
  `;
}

// ── Cross-Chain Arb Monitor ────────────────────────────────────────────────────
async function refreshCrossChainArb() {
  const el    = document.getElementById('cross-chain-arb-content');
  const badge = document.getElementById('cross-chain-arb-badge');
  if (!el) return;
  const data = await apiFetch('/cross-chain-arb');
  if (!data) { setErr('cross-chain-arb-content'); return; }

  const signal  = data.signal            || 'low';
  const opps    = data.top_opportunities || [];
  const assets  = data.assets            || {};
  const hmap    = data.arb_frequency_heatmap || {};
  const bestOpp = data.best_opportunity;

  if (badge) {
    const label = signal === 'high_opportunity' ? 'HIGH' : signal === 'moderate' ? 'MOD' : 'LOW';
    const cls   = signal === 'high_opportunity' ? 'badge-green' : signal === 'moderate' ? 'badge-yellow' : 'badge-red';
    badge.textContent   = label;
    badge.className     = 'card-badge ' + cls;
    badge.style.display = 'inline-block';
  }

  // Price grid: assets × chains
  const CHAINS = ['ETH', 'BSC', 'ARB', 'OP', 'BASE'];
  let gridHtml = '<table style="width:100%;font-size:10px;border-collapse:collapse;margin-bottom:6px">'
    + '<tr><th style="text-align:left;padding:2px 4px;color:var(--muted)">Asset</th>'
    + CHAINS.map(c => `<th style="padding:2px 4px;color:var(--muted)">${c}</th>`).join('')
    + '<th style="padding:2px 4px;color:var(--muted)">Spread</th></tr>';
  for (const [asset, aData] of Object.entries(assets)) {
    const chains   = aData.chains     || {};
    const bestSprd = aData.best_spread || {};
    const profCol  = bestSprd.is_profitable ? 'var(--green)' : 'var(--muted)';
    const buyChain = bestSprd.buy_chain;
    const sellChain= bestSprd.sell_chain;
    gridHtml += `<tr><td style="font-weight:bold;padding:2px 4px">${asset}</td>`;
    for (const c of CHAINS) {
      const p   = chains[c] ? chains[c].price : null;
      const col = c === sellChain ? 'var(--green)' : c === buyChain ? 'var(--red)' : '';
      const fmt = p == null ? '—'
        : asset === 'USDC' ? p.toFixed(4)
        : p >= 1000 ? (p / 1000).toFixed(2) + 'k'
        : p.toFixed(2);
      gridHtml += `<td style="padding:2px 4px${col ? ';color:' + col : ''}">${fmt}</td>`;
    }
    const sp = bestSprd.spread_bps != null ? bestSprd.spread_bps.toFixed(1) + ' bps' : '—';
    gridHtml += `<td style="padding:2px 4px;color:${profCol};font-weight:600">${sp}</td></tr>`;
  }
  gridHtml += '</table>';

  // Top opportunities
  let oppsHtml = '';
  for (const op of opps.slice(0, 3)) {
    const profBps = op.fee_adjusted_profit_bps ?? 0;
    const profCol = op.is_profitable ? 'var(--green)' : 'var(--red)';
    const profUsd = op.fee_adjusted_profit_usd != null ? '$' + op.fee_adjusted_profit_usd.toFixed(2) : '—';
    oppsHtml += `<div style="margin:3px 0;padding:3px 6px;background:rgba(255,255,255,0.04);border-radius:4px;font-size:10px;display:flex;gap:6px;align-items:center">
      <b style="min-width:32px">${op.asset}</b>
      <span style="color:var(--muted)">${op.bridge_route}</span>
      <span style="color:${profCol};margin-left:auto">${profBps.toFixed(1)} bps · ${profUsd}</span>
    </div>`;
  }

  // Arb frequency heatmap (24 hours × 5 chain pairs)
  const hmapCounts = hmap.counts || [];
  const hmapPairs  = hmap.chain_pairs || [];
  let hmapHtml = '<div style="margin-top:6px"><div style="font-size:9px;color:var(--muted);margin-bottom:3px">Arb Freq Heatmap · 24h · ' + hmapPairs.slice(0,3).join(' ') + '</div>'
    + '<div style="display:flex;gap:1px">';
  for (let h = 0; h < 24; h++) {
    const row   = hmapCounts[h] || [];
    const total = row.reduce((a, b) => a + b, 0);
    const alpha = Math.min(total / 30, 1) * 0.8 + 0.1;
    const bg    = `rgba(38,166,154,${alpha.toFixed(2)})`;
    const lbl   = h % 6 === 0 ? String(h) : '';
    hmapHtml += `<div title="${h}h: ${total}" style="flex:1;height:18px;background:${bg};border-radius:2px;display:flex;align-items:center;justify-content:center;font-size:7px;color:#fff">${lbl}</div>`;
  }
  hmapHtml += '</div></div>';

  const bestRoute = bestOpp ? bestOpp.route : '—';
  const bestBps   = bestOpp ? bestOpp.fee_adjusted_profit_bps.toFixed(1) + ' bps' : '—';
  const footer = `<div style="font-size:9px;color:var(--muted);margin-top:4px">best: <b style="color:var(--green)">${bestBps}</b> · ${bestRoute}</div>`;

  el.innerHTML = gridHtml
    + (oppsHtml ? `<div style="margin-bottom:4px"><div style="font-size:10px;font-weight:600;margin-bottom:2px">Top Opportunities</div>${oppsHtml}</div>` : '')
    + hmapHtml
    + footer;
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


// ── Theme Toggle ──────────────────────────────────────────────────────────────
(function initTheme() {
  const STORAGE_KEY = 'theme';
  const DEFAULT_THEME = 'dark';
  const root = document.documentElement;

  function applyTheme(theme) {
    root.setAttribute('data-theme', theme);
    const btn = document.getElementById('theme-toggle');
    if (btn) btn.textContent = theme === 'dark' ? '☀' : '🌙';
  }

  function toggleTheme() {
    const current = localStorage.getItem(STORAGE_KEY) || DEFAULT_THEME;
    const next = current === 'dark' ? 'light' : 'dark';
    localStorage.setItem(STORAGE_KEY, next);
    applyTheme(next);
  }

  // Apply saved theme on load
  const saved = localStorage.getItem(STORAGE_KEY) || DEFAULT_THEME;
  applyTheme(saved);

  document.addEventListener('DOMContentLoaded', () => {
    const btn = document.getElementById('theme-toggle');
    if (btn) btn.addEventListener('click', toggleTheme);
  });
})();

async function refreshOptionsFlowTracker() {
  const el = document.getElementById('options-flow-content');
  const badge = document.getElementById('options-flow-badge');
  if (!el) return;

  const data = await apiFetch('/options-flow-tracker');
  if (!data) { el.innerHTML = '<span class="card-badge badge-red" style="display:inline-block">Error</span>'; return; }

  const summary = data.summary || {};
  const direction = summary.net_flow_direction || 'neutral';
  const dirCol = direction === 'bullish' ? 'var(--green)' : direction === 'bearish' ? 'var(--red)' : 'var(--muted)';

  if (badge) {
    badge.textContent = direction.toUpperCase();
    badge.className = 'card-badge ' + (direction === 'bullish' ? 'badge-green' : direction === 'bearish' ? 'badge-red' : 'badge-blue');
    badge.style.display = 'inline-block';
  }

  const fmtM = v => '$' + ((v || 0) / 1e6).toFixed(2) + 'M';

  // Skew by expiry rows
  const skew_by_expiry = data.skew_by_expiry || {};
  const skewRows = Object.entries(skew_by_expiry).sort((a, b) => {
    const tv = e => (e[1].call_volume_usd || 0) + (e[1].put_volume_usd || 0);
    return tv(b) - tv(a);
  }).slice(0, 5).map(([exp, v]) => {
    const sc = v.skew_signal === 'bullish' ? 'var(--green)' : v.skew_signal === 'bearish' ? 'var(--red)' : 'var(--muted)';
    return '<tr>' +
      '<td style="font-size:9px;color:var(--text)">' + exp + '</td>' +
      '<td style="font-size:9px;text-align:right;color:var(--green)">' + fmtM(v.call_volume_usd) + '</td>' +
      '<td style="font-size:9px;text-align:right;color:var(--red)">' + fmtM(v.put_volume_usd) + '</td>' +
      '<td style="font-size:9px;text-align:right;color:' + sc + '">' + (v.skew_signal || '').toUpperCase() + '</td>' +
      '<td style="font-size:9px;text-align:right;color:var(--muted)">' + (v.skew_ratio || 0).toFixed(2) + 'x</td>' +
      '</tr>';
  }).join('');

  // Unusual flow alert rows
  const alerts = data.unusual_flow_alerts || [];
  const alertRows = alerts.slice(0, 4).map(a => {
    const ac = a.severity === 'critical' ? 'var(--red)' : '#f59e0b';
    return '<tr>' +
      '<td style="font-size:9px;color:var(--text)">' + (a.instrument || '').substring(0, 22) + '</td>' +
      '<td style="font-size:9px;text-align:right">' + fmtM(a.notional_usd) + '</td>' +
      '<td style="font-size:9px;text-align:right;color:' + (a.side === 'buy' ? 'var(--green)' : 'var(--red)') + '">' + (a.side || '').toUpperCase() + '</td>' +
      '<td style="font-size:9px;text-align:right;color:' + ac + '">' + (a.severity || '').toUpperCase() + '</td>' +
      '</tr>';
  }).join('');

  // Strike heatmap — top 5 by total notional
  const strike_heatmap = data.strike_heatmap || {};
  const hmRows = Object.entries(strike_heatmap).sort((a, b) => {
    const tn = e => (e[1].call_notional_usd || 0) + (e[1].put_notional_usd || 0);
    return tn(b) - tn(a);
  }).slice(0, 5).map(([strike, h]) => {
    const nc = (h.net_flow_usd || 0) >= 0 ? 'var(--green)' : 'var(--red)';
    const dc = h.dominant === 'call' ? 'var(--green)' : 'var(--red)';
    return '<tr>' +
      '<td style="font-size:9px;color:var(--text)">$' + Number(strike).toLocaleString() + '</td>' +
      '<td style="font-size:9px;text-align:right;color:var(--green)">' + fmtM(h.call_notional_usd) + '</td>' +
      '<td style="font-size:9px;text-align:right;color:var(--red)">' + fmtM(h.put_notional_usd) + '</td>' +
      '<td style="font-size:9px;text-align:right;color:' + nc + '">' + ((h.net_flow_usd || 0) >= 0 ? '+' : '') + fmtM(h.net_flow_usd) + '</td>' +
      '<td style="font-size:9px;text-align:right;color:' + dc + '">' + (h.dominant || '').toUpperCase() + '</td>' +
      '</tr>';
  }).join('');

  el.innerHTML =
    '<div style="display:flex;gap:12px;margin-bottom:6px;flex-wrap:wrap">' +
      '<div><div style="font-size:9px;color:var(--muted)">CALLS</div>' +
        '<div style="font-size:14px;font-weight:700;color:var(--green)">' + fmtM(summary.total_call_volume_usd) + '</div></div>' +
      '<div><div style="font-size:9px;color:var(--muted)">PUTS</div>' +
        '<div style="font-size:14px;font-weight:700;color:var(--red)">' + fmtM(summary.total_put_volume_usd) + '</div></div>' +
      '<div><div style="font-size:9px;color:var(--muted)">C/P RATIO</div>' +
        '<div style="font-size:14px;font-weight:700;color:' + dirCol + '">' + (summary.overall_skew_ratio || 0).toFixed(2) + 'x</div></div>' +
      '<div><div style="font-size:9px;color:var(--muted)">SKEW %ile</div>' +
        '<div style="font-size:14px;font-weight:700;color:' + dirCol + '">' + (summary.skew_percentile || 0).toFixed(1) + '</div></div>' +
      '<div><div style="font-size:9px;color:var(--muted)">ALERTS</div>' +
        '<div style="font-size:14px;font-weight:700;color:' + ((summary.unusual_activity_count || 0) > 0 ? '#f59e0b' : 'var(--muted)') + '">' + (summary.unusual_activity_count || 0) + '</div></div>' +
    '</div>' +
    '<div style="font-size:9px;color:var(--muted);margin-bottom:2px;font-weight:600">SKEW BY EXPIRY</div>' +
    '<table style="width:100%;border-collapse:collapse;margin-bottom:6px"><thead><tr>' +
      '<th style="font-size:8px;color:var(--muted);text-align:left;font-weight:500">EXPIRY</th>' +
      '<th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">CALLS</th>' +
      '<th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">PUTS</th>' +
      '<th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">SIGNAL</th>' +
      '<th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">RATIO</th>' +
    '</tr></thead><tbody>' + (skewRows || '<tr><td colspan="5" style="font-size:9px;color:var(--muted)">No data</td></tr>') + '</tbody></table>' +
    '<div style="font-size:9px;color:var(--muted);margin-bottom:2px;font-weight:600">UNUSUAL FLOW ALERTS</div>' +
    '<table style="width:100%;border-collapse:collapse;margin-bottom:6px"><thead><tr>' +
      '<th style="font-size:8px;color:var(--muted);text-align:left;font-weight:500">INSTRUMENT</th>' +
      '<th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">NOTIONAL</th>' +
      '<th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">SIDE</th>' +
      '<th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">SEV</th>' +
    '</tr></thead><tbody>' + (alertRows || '<tr><td colspan="4" style="font-size:9px;color:var(--muted)">No unusual flow</td></tr>') + '</tbody></table>' +
    '<div style="font-size:9px;color:var(--muted);margin-bottom:2px;font-weight:600">STRIKE HEATMAP (TOP 5)</div>' +
    '<table style="width:100%;border-collapse:collapse"><thead><tr>' +
      '<th style="font-size:8px;color:var(--muted);text-align:left;font-weight:500">STRIKE</th>' +
      '<th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">CALLS</th>' +
      '<th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">PUTS</th>' +
      '<th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">NET</th>' +
      '<th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">DOM</th>' +
    '</tr></thead><tbody>' + (hmRows || '<tr><td colspan="5" style="font-size:9px;color:var(--muted)">No data</td></tr>') + '</tbody></table>' +
    '<div style="font-size:9px;color:var(--muted);margin-top:4px">' + (data.description || '') + '</div>';
}


// ── Volatility Regime Detector ────────────────────────────────────────────────
async function refreshVolatilityRegimeDetector() {
  const el    = document.getElementById('vol-regime-content');
  const badge = document.getElementById('vol-regime-badge');
  if (!el) return;

  const data = await apiFetch('/volatility-regime-detector');
  if (!data) { setErr('vol-regime-content'); return; }

  const regime    = (data.regime || 'unknown').toLowerCase();
  const badgeMap  = { low: 'badge-green', medium: 'badge-yellow', high: 'badge-orange', extreme: 'badge-red' };
  const colorMap  = { low: 'var(--green)', medium: '#f0c040', high: '#ff8c00', extreme: 'var(--red)' };
  const badgeCls  = badgeMap[regime] || 'badge-blue';
  const regimeCol = colorMap[regime] || 'var(--text)';

  if (badge) {
    badge.textContent = regime.toUpperCase();
    badge.className = 'card-badge ' + badgeCls;
    badge.style.display = 'inline-block';
  }

  const fmt1  = v => (v == null ? '—' : v.toFixed(1) + '%');
  const fmt2  = v => (v == null ? '—' : v.toFixed(2));
  const fmt0  = v => (v == null ? '—' : v.toFixed(0));
  const fmtP  = v => (v == null ? '—' : (v * 100).toFixed(1) + '%');

  const tp    = data.transition_probability || {};
  const tpOrder = ['low', 'medium', 'high', 'extreme'];
  const tpCols  = { low: 'var(--green)', medium: '#f0c040', high: '#ff8c00', extreme: 'var(--red)' };

  const tpBars = tpOrder.map(k => {
    const pct = tp[k] != null ? (tp[k] * 100).toFixed(1) : '0.0';
    const col  = tpCols[k] || 'var(--muted)';
    return '<div style="margin-bottom:3px">' +
      '<div style="display:flex;justify-content:space-between;margin-bottom:1px">' +
        '<span style="font-size:9px;color:var(--muted)">' + k.toUpperCase() + '</span>' +
        '<span style="font-size:9px;color:' + col + '">' + pct + '%</span>' +
      '</div>' +
      '<div style="background:var(--border);border-radius:2px;height:4px;overflow:hidden">' +
        '<div style="background:' + col + ';height:100%;width:' + pct + '%;border-radius:2px"></div>' +
      '</div>' +
    '</div>';
  }).join('');

  const conf = data.regime_confidence != null ? (data.regime_confidence * 100).toFixed(0) : '—';

  el.innerHTML =
    '<div style="display:flex;gap:12px;margin-bottom:8px;flex-wrap:wrap">' +
      '<div>' +
        '<div style="font-size:8px;color:var(--muted);margin-bottom:2px">REGIME</div>' +
        '<div style="font-size:18px;font-weight:700;color:' + regimeCol + '">' + regime.toUpperCase() + '</div>' +
        '<div style="font-size:9px;color:var(--muted)">Confidence: ' + conf + '%</div>' +
      '</div>' +
      '<div>' +
        '<div style="font-size:8px;color:var(--muted);margin-bottom:2px">REALIZED VOL 30D</div>' +
        '<div style="font-size:15px;font-weight:600;color:var(--text)">' + fmt1(data.realized_vol_30d) + '</div>' +
      '</div>' +
      '<div>' +
        '<div style="font-size:8px;color:var(--muted);margin-bottom:2px">IMPLIED VOL</div>' +
        '<div style="font-size:15px;font-weight:600;color:var(--text)">' + fmt1(data.implied_vol) + '</div>' +
      '</div>' +
      '<div>' +
        '<div style="font-size:8px;color:var(--muted);margin-bottom:2px">VOL-OF-VOL</div>' +
        '<div style="font-size:15px;font-weight:600;color:var(--text)">' + fmt2(data.vol_of_vol) + '</div>' +
      '</div>' +
      '<div>' +
        '<div style="font-size:8px;color:var(--muted);margin-bottom:2px">DURATION</div>' +
        '<div style="font-size:15px;font-weight:600;color:var(--text)">' + fmt0(data.regime_duration_days) + ' days</div>' +
      '</div>' +
    '</div>' +
    '<div style="font-size:9px;color:var(--muted);font-weight:600;margin-bottom:4px">TRANSITION PROBABILITIES</div>' +
    tpBars;
}



// ── Smart Money Index ──────────────────────────────────────────────────────────
async function renderSmartMoneyIndex() {
  const el    = document.getElementById('smi-content');
  const badge = document.getElementById('smi-badge');
  if (!el) return;
  const data = await apiFetch('/smart-money-index');
  if (!data) { setErr('smi-content'); return; }

  const score  = data.smi_score ?? 0;
  const signal = data.signal || 'neutral';
  const inst   = data.institutional_flow ?? 0;
  const retail = data.retail_flow ?? 0;
  const div_   = data.divergence ?? 0;
  const comp   = data.components || {};

  // Badge
  const sigCls = signal === 'accumulation' ? 'badge-green'
               : signal === 'distribution' ? 'badge-red'
               : 'badge-blue';
  if (badge) {
    badge.textContent = signal.toUpperCase();
    badge.className = 'card-badge ' + sigCls;
    badge.style.display = '';
  }

  // Gauge bar: score in [-1, 1] → 0..100%
  const pct = ((score + 1) / 2 * 100).toFixed(1);
  const gaugeCol = score > 0.2 ? 'var(--green)' : score < -0.2 ? 'var(--red)' : 'var(--muted)';

  const fmtFlow = v => (v >= 0 ? '+' : '') + (v / 1e3).toFixed(1) + 'B';

  el.innerHTML =
    '<div style="margin-bottom:6px">' +
      '<div style="display:flex;justify-content:space-between;margin-bottom:2px">' +
        '<span style="font-size:9px;color:var(--muted)">SMI SCORE</span>' +
        '<span style="font-size:13px;font-weight:700;color:' + gaugeCol + '">' + (score > 0 ? '+' : '') + score.toFixed(4) + '</span>' +
      '</div>' +
      '<div style="height:6px;background:var(--bg2);border-radius:3px;overflow:hidden">' +
        '<div style="width:' + pct + '%;height:100%;background:' + gaugeCol + ';border-radius:3px;transition:width 0.4s"></div>' +
      '</div>' +
      '<div style="display:flex;justify-content:space-between;font-size:8px;color:var(--muted);margin-top:1px">' +
        '<span>-1 Distribution</span><span>Neutral</span><span>Accumulation +1</span>' +
      '</div>' +
    '</div>' +
    '<div style="display:flex;gap:10px;margin-bottom:6px;flex-wrap:wrap">' +
      '<div><div style="font-size:9px;color:var(--muted)">INST FLOW</div>' +
        '<div style="font-size:12px;font-weight:700;color:' + (inst >= 0 ? 'var(--green)' : 'var(--red)') + '">' + fmtFlow(inst) + '</div></div>' +
      '<div><div style="font-size:9px;color:var(--muted)">RETAIL FLOW</div>' +
        '<div style="font-size:12px;font-weight:700;color:' + (retail >= 0 ? 'var(--green)' : 'var(--red)') + '">' + fmtFlow(retail) + '</div></div>' +
      '<div><div style="font-size:9px;color:var(--muted)">DIVERGENCE</div>' +
        '<div style="font-size:12px;font-weight:700;color:' + (div_ >= 0 ? 'var(--green)' : 'var(--red)') + '">' + fmtFlow(div_) + '</div></div>' +
    '</div>' +
    '<div style="font-size:9px;color:var(--muted);font-weight:600;margin-bottom:3px">COMPONENTS</div>' +
    '<table style="width:100%;border-collapse:collapse"><tbody>' +
      '<tr><td style="font-size:9px;color:var(--muted)">Block Ratio</td>' +
        '<td style="font-size:9px;text-align:right;color:var(--text)">' + ((comp.block_ratio || 0) * 100).toFixed(1) + '%</td></tr>' +
      '<tr><td style="font-size:9px;color:var(--muted)">OI Skew</td>' +
        '<td style="font-size:9px;text-align:right;color:' + ((comp.oi_skew || 0) >= 0 ? 'var(--green)' : 'var(--red)') + '">' + (comp.oi_skew >= 0 ? '+' : '') + (comp.oi_skew || 0).toFixed(4) + '</td></tr>' +
      '<tr><td style="font-size:9px;color:var(--muted)">Futures Basis</td>' +
        '<td style="font-size:9px;text-align:right;color:' + ((comp.futures_basis || 0) >= 0 ? 'var(--green)' : 'var(--red)') + '">' + (comp.futures_basis >= 0 ? '+' : '') + (comp.futures_basis || 0).toFixed(2) + '%</td></tr>' +
      '<tr><td style="font-size:9px;color:var(--muted)">Whale Accum</td>' +
        '<td style="font-size:9px;text-align:right;color:' + ((comp.whale_accumulation || 0) >= 0 ? 'var(--green)' : 'var(--red)') + '">' + (comp.whale_accumulation >= 0 ? '+' : '') + ((comp.whale_accumulation || 0) / 1e3).toFixed(1) + 'k BTC</td></tr>' +
    '</tbody></table>';
}

// ── Order Flow Toxicity (VPIN) ────────────────────────────────────────────────
async function renderOrderFlowToxicity() {
  const el = document.getElementById('order-flow-toxicity-content');
  const badge = document.getElementById('order-flow-toxicity-badge');
  if (!el) return;
  const data = await fetchJSON('/api/order-flow-toxicity');
  if (!data) { el.innerHTML = '<span class="card-badge badge-red">Error</span>'; return; }

  const vpin = data.vpin_score != null ? (data.vpin_score * 100).toFixed(1) : '—';
  const toxicity = data.toxicity_level || '—';
  const badgeColor = {low: 'badge-green', medium: 'badge-yellow', high: 'badge-orange', extreme: 'badge-red'}[toxicity] || 'badge-blue';
  const signal = data.informed_trading_signal || '—';
  const buyPct = data.buy_volume_frac != null ? (data.buy_volume_frac * 100).toFixed(1) : '—';
  const sellPct = data.sell_volume_frac != null ? (data.sell_volume_frac * 100).toFixed(1) : '—';
  const vpinNum = data.vpin_score != null ? data.vpin_score : 0;
  const gaugeWidth = Math.round(vpinNum * 100);

  if (badge) {
    badge.textContent = toxicity.toUpperCase();
    badge.className = 'card-badge ' + badgeColor;
    badge.style.display = 'inline-block';
  }

  // Rolling VPIN sparkline (50 values → mini bar chart)
  const rolling = data.rolling_vpin_50 || [];
  const sparkBars = rolling.map(v => {
    const h = Math.round(v * 40);
    const color = v >= 0.75 ? '#ef4444' : v >= 0.50 ? '#f97316' : v >= 0.25 ? '#eab308' : '#22c55e';
    return `<span style="display:inline-block;width:3px;height:${h}px;background:${color};margin-right:1px;vertical-align:bottom;border-radius:1px"></span>`;
  }).join('');

  el.innerHTML =
    '<div style="display:flex;align-items:center;gap:12px;margin-bottom:8px">' +
      '<div style="flex:1">' +
        '<div style="font-size:9px;color:var(--muted);font-weight:600;margin-bottom:2px">VPIN SCORE</div>' +
        '<div style="font-size:22px;font-weight:700;line-height:1">' + vpin + '<span style="font-size:11px;color:var(--muted)">%</span></div>' +
        '<div style="font-size:9px;color:var(--muted);margin-top:2px">Signal: ' + signal.replace(/_/g, ' ') + '</div>' +
      '</div>' +
      '<div style="flex:1">' +
        '<div style="font-size:9px;color:var(--muted);font-weight:600;margin-bottom:4px">TOXICITY GAUGE</div>' +
        '<div style="background:var(--card-bg,#1e2130);border-radius:4px;height:8px;overflow:hidden">' +
          '<div style="height:100%;width:' + gaugeWidth + '%;background:' +
            (vpinNum >= 0.75 ? '#ef4444' : vpinNum >= 0.50 ? '#f97316' : vpinNum >= 0.25 ? '#eab308' : '#22c55e') +
          ';border-radius:4px;transition:width 0.4s"></div>' +
        '</div>' +
        '<div style="display:flex;justify-content:space-between;font-size:8px;color:var(--muted);margin-top:2px"><span>0</span><span>Low</span><span>Med</span><span>High</span><span>100</span></div>' +
      '</div>' +
    '</div>' +
    '<div style="font-size:9px;color:var(--muted);font-weight:600;margin-bottom:3px">BUY vs SELL VOLUME</div>' +
    '<div style="display:flex;height:10px;border-radius:4px;overflow:hidden;margin-bottom:6px">' +
      '<div style="width:' + buyPct + '%;background:#22c55e" title="Buy ' + buyPct + '%"></div>' +
      '<div style="width:' + sellPct + '%;background:#ef4444" title="Sell ' + sellPct + '%"></div>' +
    '</div>' +
    '<div style="display:flex;justify-content:space-between;font-size:9px;margin-bottom:8px">' +
      '<span style="color:#22c55e">▲ Buy ' + buyPct + '%</span>' +
      '<span style="color:#ef4444">▼ Sell ' + sellPct + '%</span>' +
    '</div>' +
    '<div style="font-size:9px;color:var(--muted);font-weight:600;margin-bottom:3px">ROLLING VPIN (50 buckets)</div>' +
    '<div style="display:flex;align-items:flex-end;height:42px;padding:2px 0">' + sparkBars + '</div>';
}


// ── Liquidation Cascade Detector ─────────────────────────────────────────────
async function renderLiqCascadeDetector() {
  const el    = document.getElementById('liq-cascade-detector-content');
  const badge = document.getElementById('liq-cascade-detector-badge');
  if (!el) return;
  const data = await apiFetch('/liquidation-cascade-detector');
  if (!data) { setErr('liq-cascade-detector-content'); return; }

  const prob = (data.cascade_probability ?? 0);
  const probPct = Math.round(prob * 100);
  const regime = (data.regime || 'calm').toLowerCase();
  const regimeColors = { calm: '#22c55e', building: '#f59e0b', cascade: '#ef4444', peak: '#dc2626' };
  const regimeColor = regimeColors[regime] || '#888';

  if (badge) {
    badge.style.display = 'inline-block';
    badge.textContent = regime.toUpperCase();
    badge.style.background = regimeColor;
    badge.style.color = '#fff';
    badge.style.padding = '1px 6px';
    badge.style.borderRadius = '4px';
    badge.style.fontSize = '9px';
    badge.style.fontWeight = '600';
  }

  const totalLiq = data.total_liquidated_usd ?? 0;
  const liqVel = data.liq_velocity ?? 0;
  const timeMin = data.time_to_cascade_minutes ?? 0;
  const exchanges = (data.exchanges || []).join(', ');
  const supportLevels = (data.support_levels || []).slice(0, 6);
  const chain = (data.cascade_chain || []).slice(0, 5);

  const supportRows = supportLevels.map(lvl =>
    `<span style="display:inline-block;margin:1px 3px;font-size:9px;color:var(--muted)">$${lvl.toLocaleString()}</span>`
  ).join('');

  const chainRows = chain.map(e =>
    `<tr>
      <td style="padding:1px 3px;font-size:9px">${e.asset}</td>
      <td style="padding:1px 3px;font-size:9px;text-align:right">$${(e.amount/1e6).toFixed(1)}M</td>
      <td style="padding:1px 3px;font-size:9px;text-align:right;color:var(--muted)">t+${e.time}m</td>
    </tr>`
  ).join('');

  el.innerHTML =
    `<div style="margin-bottom:6px">` +
      `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:2px">` +
        `<span style="font-size:9px;color:var(--muted);font-weight:600">CASCADE PROBABILITY</span>` +
        `<span style="font-size:11px;font-weight:700;color:${regimeColor}">${probPct}%</span>` +
      `</div>` +
      `<div style="background:var(--border);border-radius:3px;height:8px;overflow:hidden">` +
        `<div id="cascade-probability-bar" style="height:100%;width:${probPct}%;background:${regimeColor};transition:width 0.5s;border-radius:3px"></div>` +
      `</div>` +
    `</div>` +
    `<div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;margin-bottom:6px">` +
      `<div><span style="font-size:8px;color:var(--muted)">TIME TO CASCADE</span><br><span style="font-size:10px;font-weight:600">${timeMin.toFixed(1)} min</span></div>` +
      `<div><span style="font-size:8px;color:var(--muted)">LIQ VELOCITY</span><br><span style="font-size:10px;font-weight:600">$${(liqVel/1e6).toFixed(1)}M/min</span></div>` +
      `<div><span style="font-size:8px;color:var(--muted)">TOTAL LIQUIDATED</span><br><span style="font-size:10px;font-weight:600">$${(totalLiq/1e9).toFixed(2)}B</span></div>` +
      `<div><span style="font-size:8px;color:var(--muted)">EXCHANGES</span><br><span style="font-size:9px">${exchanges}</span></div>` +
    `</div>` +
    `<div style="margin-bottom:6px">` +
      `<div style="font-size:9px;color:var(--muted);font-weight:600;margin-bottom:2px">SUPPORT LEVELS</div>` +
      `<div style="line-height:1.6">${supportRows || '<span style="font-size:9px;color:var(--muted)">–</span>'}</div>` +
    `</div>` +
    `<div>` +
      `<div style="font-size:9px;color:var(--muted);font-weight:600;margin-bottom:2px">CASCADE CHAIN (TOP 5)</div>` +
      `<table style="width:100%;border-collapse:collapse">` +
        `<thead><tr>` +
          `<th style="font-size:8px;color:var(--muted);text-align:left;font-weight:500">ASSET</th>` +
          `<th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">AMOUNT</th>` +
          `<th style="font-size:8px;color:var(--muted);text-align:right;font-weight:500">TIME</th>` +
        `</tr></thead>` +
        `<tbody>${chainRows || '<tr><td colspan="3" style="font-size:9px;color:var(--muted)">No data</td></tr>'}</tbody>` +
      `</table>` +
    `</div>`;
}


// ── Bootstrap on Load ──────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', init);


