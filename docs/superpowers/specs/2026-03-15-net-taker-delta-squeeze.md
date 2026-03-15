# Spec: Net Taker Delta + Short Squeeze Detector

## Goal
Identify short squeeze setups: OI rising during price crash + funding normalizing pattern.

## Features

### 1) Net Taker Delta (per symbol)
- API: `/api/net-taker-delta?symbol=X&window=60` (minutes)
- Sum of buy-taker vol minus sell-taker vol over window
- Stored per 1-min bucket, aggregated
- Frontend: stacked bar chart (buy=green, sell=red), net line overlay

### 2) OI Surge Detector
- Detect when OI increases >20% while price decreases >10% in same window
- This = shorts piling in (bearish OI accumulation)
- Flag: `oi_surge_with_crash` = True/False

### 3) Funding Normalization Alert
- After `oi_surge_with_crash` event: track funding recovery
- If funding goes from < -0.5% back toward 0 within 2 hours → `squeeze_signal`
- Alert card: "⚡ Short Squeeze Setup: LYNUSDT — OI +65% during crash, funding normalizing"

## Data Sources
- OI: existing OI poller (already in DB)
- Funding: existing funding poller
- Taker delta: existing trades table (has `is_buyer_maker`)

## Implementation Notes
- All data already collected — just needs API endpoint + frontend card
- Combine existing columns: `is_buyer_maker` in trades → taker side
- OI velocity already being polled every minute
