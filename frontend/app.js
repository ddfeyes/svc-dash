'use strict';

// ── Config ────────────────────────────────────────────────────────────────────
const API = window.location.protocol + '//' + window.location.hostname + ':8765/api';
const WS  = 'ws://' + window.location.hostname + ':8765';

const REFRESH_MS   = 5000;   // poll interval
const TRADE_MAX    = 100;    // max rows in tape
const WHALE_USD    = 10000;  // highlight threshold

// ── State ─────────────────────────────────────────────────────────────────────
let activeSymbol  = null;
let allSymbols    = [];
let priceChart    = null;   // TradingView Lightweight Charts instance
let oiChart       = null;   // Chart.js
let cvdChart      = null;   // Chart.js
let fundingChart  = null;   // Chart.js
let flowChart     = null;   // Chart.js
let wsAlerts      = null;
let refreshTimer  = null;
let _lastPrice    = 1;      // used for OI USDT conversion

// ── Helpers ───────────────────────────────────────────────────────────────────
async function apiFetch(path) {
  const url = API + path;
  try {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  } catch (e) {
    console.warn('[apiFetch]', path, e.message);
    return null;
  }
}

// Smart price formatting: 6 decimals for sub-penny assets
function fmtPrice(n) {
  if (n == null) return '—';
  const v = parseFloat(n);
  if (isNaN(v)) return '—';
  if (v < 0.01) return v.toFixed(6);
  if (v < 1)    return v.toFixed(4);
  if (v < 100)  return v.toFixed(3);
  return v.toFixed(2);
}

function fmt(n, decimals = 4) {
  if (n == null) return '—';
  const v = parseFloat(n);
  if (isNaN(v)) return '—';
  return v.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

// K/M/B formatting with sign preservation
function fmtK(n) {
  if (n == null) return '—';
  const raw = parseFloat(n);
  if (isNaN(raw)) return '—';
  const abs = Math.abs(raw);
  const sign = raw < 0 ? '-' : '';
  if (abs >= 1e9) return sign + (abs / 1e9).toFixed(2) + 'B';
  if (abs >= 1e6) return sign + (abs / 1e6).toFixed(2) + 'M';
  if (abs >= 1e3) return sign + (abs / 1e3).toFixed(1) + 'k';
  return raw.toFixed(2);
}

function fmtUsd(n) {
  return '$' + fmtK(n);
}

function fmtTime(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function fmtTimeShort(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit' });
}

function pctColor(v) {
  return v > 0 ? 'var(--green)' : v < 0 ? 'var(--red)' : 'var(--muted)';
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
    wickDownColor: '#ff4d4f',
  });

  priceChart._volumeSeries = priceChart.addHistogramSeries({
    color: 'rgba(78,168,222,0.3)',
    priceFormat: { type: 'volume' },
    priceScaleId: 'vol',
  });
  priceChart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });

  const ro = new ResizeObserver(entries => {
    for (const e of entries) priceChart.applyOptions({ width: e.contentRect.width });
  });
  ro.observe(container);
}

function mkLineChart(canvasId, color, yFmtFn) {
  const canvas = document.getElementById(canvasId);
  if (!canvas || !window.Chart) return null;
  return new Chart(canvas, {
    type: 'line',
    data: { labels: [], datasets: [{ data: [], borderColor: color, backgroundColor: color.replace(')', ',0.08)').replace('rgb','rgba'), borderWidth: 1.5, pointRadius: 0, tension: 0.3, fill: true }] },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          mode: 'index', intersect: false,
          backgroundColor: '#1c2030', titleColor: '#6b7280', bodyColor: '#e2e8f0',
          borderColor: 'rgba(255,255,255,0.08)', borderWidth: 1,
          callbacks: { label: ctx => ' ' + (yFmtFn ? yFmtFn(ctx.raw) : ctx.raw) },
        },
      },
      scales: {
        x: { ticks: { color: '#6b7280', font: { size: 9 }, maxTicksLimit: 6, maxRotation: 0 }, grid: { color: 'rgba(255,255,255,0.04)' } },
        y: { ticks: { color: '#6b7280', font: { size: 9 }, callback: v => yFmtFn ? yFmtFn(v) : fmtK(v) }, grid: { color: 'rgba(255,255,255,0.04)' } },
      },
    },
  });
}

function initOiChart()      { oiChart      = mkLineChart('oi-canvas',      '#4ea8de', v => fmtUsd(v)); }
function initCvdChart()     { cvdChart     = mkLineChart('cvd-canvas',     '#ab7df8', v => fmtK(v)); }
function initFundingChart() { fundingChart = mkLineChart('funding-canvas', '#f0c040', v => (v * 100).toFixed(4) + '%'); }
function initFlowChart()    { flowChart    = mkLineChart('flow-canvas',    '#4ea8de', v => (v * 100).toFixed(1) + '%'); }

function resetCharts() {
  _lastTradeId = null;
  if (priceChart?._candleSeries) {
    priceChart._candleSeries.setData([]);
    priceChart._volumeSeries.setData([]);
  }
  [oiChart, cvdChart, fundingChart, flowChart].forEach(ch => {
    if (ch) { ch.data.labels = []; ch.data.datasets[0].data = []; ch.update('none'); }
  });
  const tape = document.getElementById('trade-tape');
  if (tape) tape.innerHTML = '';
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
    priceChart._candleSeries.setData(candles);
    priceChart._volumeSeries.setData(volumes);
    document.getElementById('price-chart-container').classList.remove('empty');

    const last = candles[candles.length - 1];
    const prev = candles.length > 1 ? candles[candles.length - 2] : null;
    _lastPrice = last.close || 1;
    updateLastPrice(last.close, prev ? last.close - prev.close : 0);
  }
}

function updateLastPrice(price, change) {
  const priceEl  = document.getElementById('last-price');
  const changeEl = document.getElementById('price-change');
  if (priceEl)  priceEl.textContent = fmtPrice(price);
  if (changeEl) {
    const sign = change >= 0 ? '+' : '';
    changeEl.textContent = sign + fmtPrice(change);
    changeEl.className = change >= 0 ? 'up' : 'down';
  }
}

// ── Render: OI Chart (in USDT) ────────────────────────────────────────────────
async function renderOiChart() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/oi/history?limit=200&symbol=${sym}`);
  if (!data?.data?.length || !oiChart) return;

  // Prefer Binance data
  const rows = data.data.filter(d => d.exchange === 'binance');
  const src  = rows.length ? rows : data.data;

  // oi_value is in contracts (tokens) — multiply by current price for USDT
  const price  = _lastPrice || 1;
  const labels = src.map(d => fmtTimeShort(d.ts));
  const values = src.map(d => parseFloat(d.oi_value) * price);

  oiChart.data.labels = labels;
  oiChart.data.datasets[0].data = values;
  oiChart.update('none');

  const first  = values[0] || 0;
  const last   = values[values.length - 1] || 0;
  const pct    = first ? ((last - first) / first * 100) : 0;
  const pctStr = (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%';

  const el = document.getElementById('oi-metrics');
  if (el) {
    el.innerHTML = `
      <span class="oi-stat">OI <span style="color:${pctColor(pct)}">${fmtUsd(last)}</span></span>
      <span class="oi-stat">Δ <span style="color:${pctColor(pct)}">${pctStr}</span></span>
      <span class="oi-stat">exchange <span>${src[0]?.exchange || '—'}</span></span>
    `;
  }
}

// ── Render: CVD Chart ─────────────────────────────────────────────────────────
async function renderCvdChart() {
  if (!cvdChart) return;
  const sym  = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/cvd/history?window=3600&symbol=${sym}`);
  if (!data?.data?.length) return;

  const labels = data.data.map(d => fmtTimeShort(d.ts));
  const values = data.data.map(d => parseFloat(d.cvd));

  cvdChart.data.labels = labels;
  cvdChart.data.datasets[0].data = values;
  const lastCvd = values[values.length - 1] || 0;
  cvdChart.data.datasets[0].borderColor = lastCvd >= 0 ? '#ab7df8' : '#ff4d4f';
  cvdChart.data.datasets[0].backgroundColor = lastCvd >= 0 ? 'rgba(171,125,248,0.08)' : 'rgba(255,77,79,0.08)';
  cvdChart.update('none');

  const el = document.getElementById('cvd-metrics');
  if (el) {
    const col = pctColor(lastCvd);
    el.innerHTML = `<span class="oi-stat">CVD <span style="color:${col}">${fmtK(lastCvd)}</span></span>`;
  }
}

// ── Render: Funding Rate Chart ────────────────────────────────────────────────
async function renderFundingChart() {
  if (!fundingChart) return;
  const sym  = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/funding/history?limit=100&symbol=${sym}`);
  if (!data?.data?.length) return;

  // Prefer Binance
  const rows = data.data.filter(d => d.exchange === 'binance');
  const src  = rows.length ? rows : data.data;

  const labels = src.map(d => fmtTimeShort(d.ts));
  const values = src.map(d => parseFloat(d.rate));

  fundingChart.data.labels = labels;
  fundingChart.data.datasets[0].data = values;
  const lastRate = values[values.length - 1] || 0;
  const rateCol = lastRate > 0.001 ? 'var(--green)' : lastRate < -0.001 ? 'var(--red)' : 'var(--muted)';
  fundingChart.data.datasets[0].borderColor = lastRate >= 0 ? '#f0c040' : '#ff4d4f';
  fundingChart.update('none');

  const el = document.getElementById('funding-metrics');
  if (el) {
    el.innerHTML = `<span class="oi-stat">Rate <span style="color:${rateCol}">${(lastRate * 100).toFixed(4)}%</span></span>`;
  }
}

// ── Render: Flow/Volume Imbalance ─────────────────────────────────────────────
async function renderFlowChart() {
  if (!flowChart) return;
  const sym  = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/flow-imbalance?window=3600&bucket_size=60&symbol=${sym}`);
  if (!data?.series?.length) return;

  const labels = data.series.map(d => fmtTimeShort(d.ts));
  const values = data.series.map(d => parseFloat(d.ratio));

  flowChart.data.labels = labels;
  flowChart.data.datasets[0].data = values;

  // Color points by label
  const colors = data.series.map(d =>
    d.label === 'buy'  ? 'rgba(0,224,130,0.7)' :
    d.label === 'sell' ? 'rgba(255,77,79,0.7)' :
    'rgba(78,168,222,0.5)'
  );
  flowChart.data.datasets[0].backgroundColor = colors;
  flowChart.update('none');

  const el = document.getElementById('flow-metrics');
  if (el && data.summary) {
    const s = data.summary;
    const biasCol = s.bias === 'buy' ? 'var(--green)' : s.bias === 'sell' ? 'var(--red)' : 'var(--muted)';
    el.innerHTML = `
      <span class="oi-stat">Bias <span style="color:${biasCol}">${s.bias}</span></span>
      <span class="oi-stat">Strength <span>${s.bias_strength?.toFixed(1)}%</span></span>
      <span class="oi-stat">Buy vol <span style="color:var(--green)">${fmtK(s.total_buy_vol)}</span></span>
      <span class="oi-stat">Sell vol <span style="color:var(--red)">${fmtK(s.total_sell_vol)}</span></span>
    `;
  }
}

// ── Render: Trade Tape (amounts in USDT) ──────────────────────────────────────
let _lastTradeId = null;

async function renderTradeTape() {
  const sym = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/trades/recent?limit=50&symbol=${sym}`);
  if (!data?.data?.length) return;

  const tape   = document.getElementById('trade-tape');
  const trades = data.data;

  const lastId   = _lastTradeId;
  const newTrades = lastId ? trades.filter(t => t.id > lastId) : trades;
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
        <span class="trade-qty">${fmtUsd(usd)}</span>
        <span class="trade-time">${fmtTime(t.ts)}</span>
      </div>`;
  }).join('');

  tape.insertAdjacentHTML('afterbegin', rows);

  const children = tape.children;
  while (children.length > TRADE_MAX) tape.removeChild(children[children.length - 1]);
}

// ── Render: Phase / Market Regime ─────────────────────────────────────────────
async function renderPhase() {
  const sym  = encodeURIComponent(activeSymbol);
  const data = await apiFetch(`/market-regime?symbol=${sym}`);
  if (!data) return;

  const phase  = data.phase  || data.regime || 'Unknown';
  const regime = data.regime || phase;
  const conf   = data.phase_confidence != null ? (data.phase_confidence * 100).toFixed(0) + '%' : '—';
  const score  = data.score  != null ? data.score.toFixed(1) : '—';
  const action = data.action || '—';

  const phaseColors = {
    accumulation: 'var(--green)',  distribution: 'var(--red)',
    markup:       'var(--blue)',   markdown:     'var(--red)',
    ranging:      'var(--yellow)', bear:         'var(--red)',
    bull:         'var(--green)',
  };
  const color      = phaseColors[(phase || '').toLowerCase()] || 'var(--fg)';
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

// ── WebSocket: Alerts ─────────────────────────────────────────────────────────
function connectAlerts() {
  if (wsAlerts) { wsAlerts.close(); wsAlerts = null; }

  const url = WS + '/ws/alerts';
  try { wsAlerts = new WebSocket(url); }
  catch (e) { console.warn('[WS] failed:', e); return; }

  const statusEl = document.getElementById('header-status');

  wsAlerts.onopen = () => {
    console.log('[WS] connected');
    if (statusEl) { statusEl.textContent = 'connected'; statusEl.className = 'connected'; }
  };
  wsAlerts.onmessage = (evt) => {
    try { const msg = JSON.parse(evt.data); showAlert(msg.description || msg.message || JSON.stringify(msg)); }
    catch (_) {}
  };
  wsAlerts.onclose = () => {
    if (statusEl) { statusEl.textContent = 'disconnected — reconnecting…'; statusEl.className = 'disconnected'; }
    setTimeout(connectAlerts, 5000);
  };
  wsAlerts.onerror = (e) => console.warn('[WS] error', e);
}

function showAlert(text) {
  const bar = document.getElementById('alert-bar');
  if (!bar) return;
  bar.textContent = '⚡ ' + text;
  bar.classList.add('visible');
  clearTimeout(bar._timeout);
  bar._timeout = setTimeout(() => bar.classList.remove('visible'), 8000);
}

// ── Main Refresh Loop ─────────────────────────────────────────────────────────
async function refresh() {
  if (!activeSymbol) return;

  // renderPriceChart first so _lastPrice is set before OI USDT conversion
  await renderPriceChart();
  await Promise.all([
    renderOiChart(),
    renderCvdChart(),
    renderFundingChart(),
    renderFlowChart(),
    renderTradeTape(),
    renderPhase(),
  ]);
}

// ── Bootstrap ─────────────────────────────────────────────────────────────────
async function init() {
  initPriceChart();
  initOiChart();
  initCvdChart();
  initFundingChart();
  initFlowChart();
  connectAlerts();

  await loadSymbols();
  await refresh();

  refreshTimer = setInterval(refresh, REFRESH_MS);
}

document.addEventListener('DOMContentLoaded', init);
