# Spec: Smart Money Divergence Detector

## Goal
Detect when large trades (>$10k each, "smart money") trade in the **opposite direction** from
retail flow (all trades <$10k). Divergence = institutional accumulation/distribution into retail panic.

## Definition

**Retail CVD**: cumulative delta of trades with `price * qty < 10000 USD`
**Smart CVD**: cumulative delta of trades with `price * qty >= 10000 USD`

**Divergence signal**: `smart_money_direction != retail_direction` over a rolling window

Examples:
- Smart buying + retail selling = institutional accumulation (bullish)
- Smart selling + retail buying = institutional distribution (bearish)

## API

### `GET /api/smart-money-divergence?symbol=X&window=1800`
- `window`: seconds (default 1800 = 30 min)
- `threshold_usd`: large trade threshold (default 10000)
- Returns:
```json
{
  "symbol": "BANANAS31USDT",
  "window_seconds": 1800,
  "threshold_usd": 10000,
  "smart_cvd": 123456.78,       // net USD, positive = net buy
  "retail_cvd": -234567.89,     // net USD, positive = net buy
  "smart_trade_count": 42,
  "retail_trade_count": 1803,
  "divergence_score": 0.87,     // -1 to 1, abs value = magnitude
  "signal": "accumulation",     // "accumulation" | "distribution" | "aligned" | "neutral"
  "smart_pct": 0.35,            // smart volume as % of total volume
  "divergence_detected": true,
  "buckets": [                  // 5-min buckets for chart
    {"ts": 1710000000, "smart_cvd": ..., "retail_cvd": ...},
    ...
  ]
}
```

### `GET /api/smart-money-divergence/all?window=1800`
- Returns divergence for all tracked symbols (leaderboard)

## Divergence Score Formula

```python
total_vol = abs(smart_cvd) + abs(retail_cvd) + 1e-8
divergence_score = (smart_cvd - retail_cvd) / total_vol  # normalized -1..1
```

Signal thresholds:
- `|divergence_score| < 0.15` → "neutral"
- `divergence_score > 0.15` → "accumulation" (smart buying > retail buying)
- `divergence_score < -0.15` → "distribution" (smart selling > retail selling)
- If smart and retail same direction: "aligned"

## Frontend Card

**Position**: after Net Taker Delta card (already exists)

**Layout**:
```
┌─ Smart Money Divergence ──────────────────────────────┐
│  Symbol tabs: BANANAS31 | COS | DEXE | LYN            │
│                                                        │
│  Signal badge: ⬆️ ACCUMULATION (score: 0.87)          │
│  Smart $ net: +$123k  Retail $ net: -$234k            │
│  Smart %: 35% of volume                               │
│                                                        │
│  [Dual-line chart: smart_cvd vs retail_cvd over 30m]  │
│  Green line = smart, Orange line = retail             │
│  Divergence zone shaded when signals differ           │
└───────────────────────────────────────────────────────┘
```

Chart: Chart.js line chart, 5-min buckets, 30-min window default

## Implementation Notes

### Data source
- Existing `trades` table: `price`, `qty`, `side` (buy/sell), `ts`, `symbol`
- Compute `value_usd = price * qty` on the fly
- No new DB columns needed

### Backend function (metrics.py)
```python
async def smart_money_divergence(symbol: str, window_seconds: int = 1800, threshold_usd: float = 10000):
    since = time.time() - window_seconds
    trades = await get_trades_for_cvd(since, symbol=symbol)

    smart_buy = smart_sell = retail_buy = retail_sell = 0.0
    buckets = defaultdict(lambda: {"smart_buy": 0, "smart_sell": 0, "retail_buy": 0, "retail_sell": 0})
    bucket_size = 300  # 5 min

    for t in trades:
        val = t["price"] * t["qty"]
        is_buy = t["side"].lower() in ("buy",)
        bucket_ts = int(t["ts"] // bucket_size) * bucket_size
        if val >= threshold_usd:
            if is_buy: smart_buy += val; buckets[bucket_ts]["smart_buy"] += val
            else:      smart_sell += val; buckets[bucket_ts]["smart_sell"] += val
        else:
            if is_buy: retail_buy += val; buckets[bucket_ts]["retail_buy"] += val
            else:      retail_sell += val; buckets[bucket_ts]["retail_sell"] += val

    smart_cvd = smart_buy - smart_sell
    retail_cvd = retail_buy - retail_sell
    ...
```

### Tests
- `tests/test_smart_money_divergence.py`
- Test: all large buys → accumulation signal
- Test: all large sells + retail buys → distribution signal
- Test: balanced → neutral
- Test: bucket aggregation correct
- Test: API endpoint returns correct schema

## Existing code to reuse
- `get_trades_for_cvd()` in storage.py — fetches all trades in window
- `detect_large_trades()` in metrics.py — skeleton for size filtering
- Net Taker Delta endpoint structure (added 2026-03-15) — follow same pattern

## Commit convention
```
feat: add smart money divergence detector

- /api/smart-money-divergence (per-symbol + all)  
- signal: accumulation/distribution/aligned/neutral
- Chart.js dual-line with divergence shading
- Tests pass: N/N
```
