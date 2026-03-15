'use strict';

// ── Config ────────────────────────────────────────────────────────────────────
const API = window.location.protocol + '//' + window.location.hostname + ':8765/api';
const WS  = 'ws://' + window.location.hostname + ':8765';

const REFRESH_MS   = 5000;   // poll interval
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
let spreadChart  = null;   // Chart.js
let wsAlerts     = null;
let refreshTimer = null;
let _lastPrice   = null;   // most recent close price (for OI USDT calc)

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

/** Format a large number as $47.5M, $1.2k, etc. */
function fmtUSD(n) {
  if (n == null) return '—';
  const v = parseFloat(n);
  if (isNaN(v)) return '—';
  const abs = Math.abs(v);
  const sign = v < 0 ? '-' : '';
  if (abs >= 1e9) return sign + '$' + (abs / 1e9).toFixed(1) + 'B';
  if (abs >= 1e6) return sign + '$' + (abs / 1e6).toFixed(1) + 'M';
  if (abs >= 1e3) return sign + '$' + (abs / 1e3).toFixed(1) + 'k';
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
    wickDownColor: '#ff4d4f',
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

  document.getElementById('trade-tape').innerHTML = '';
  document.getElementById('cvd-metrics').innerHTML = '';
  document.getElementById('funding-metrics').innerHTML = '';
  document.getElementById('spread-metrics').innerHTML = '';
  document.getElementById('vol-imbalance-content').innerHTML = '';
  document.getElementById('oi-metrics').innerHTML = '';
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
    updateLastPrice(last.close, prev ? last.close - prev.close : 0);
  }
}

function updateLastPrice(price, change) {
  _lastPrice = price;  // store for OI USDT calculation
  const priceEl  = document.getElementById('last-price');
  const changeEl = document.getElementById('price-change');
  if (priceEl) priceEl.textContent = fmtPrice(price);
  if (changeEl) {
    const sign = change >= 0 ? '+' : '';
    changeEl.textContent = sign + fmtPrice(change);
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
  // Multiply raw OI (coins) by current price to get USDT value
  const price = _lastPrice || 1;
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
  if (!data) return;

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
  if (!data) return;

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
  if (!data) return;

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

  const url = WS + '/ws/alerts';
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

// ── Main Refresh Loop ─────────────────────────────────────────────────────────
async function refresh() {
  if (!activeSymbol) return;

  await Promise.all([
    renderPriceChart(),
    renderOiChart(),
    renderCvdChart(),
    renderFunding(),
    renderSpread(),
    renderTradeTape(),
    renderVolumeImbalance(),
    renderPhase(),
    renderOiDivergence(),
    renderMicrostructure(),
  ]);
}

// ── Bootstrap ─────────────────────────────────────────────────────────────────
async function init() {
  initPriceChart();
  initOiChart();
  initCvdChart();
  initFundingChart();
  initSpreadChart();
  connectAlerts();

  await loadSymbols();
  await refresh();

  refreshTimer = setInterval(refresh, REFRESH_MS);
}

document.addEventListener('DOMContentLoaded', init);
