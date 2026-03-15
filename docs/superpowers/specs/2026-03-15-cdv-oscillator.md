# CDV vs Price Action Oscillator

**Date:** 2026-03-15  
**Feature:** Cumulative Delta Volume (CDV) vs Price Oscillator

## Problem
Traders need to see when CDV makes a new high/low that price fails to confirm — a leading divergence signal before reversals.

## Approach
1. Backend `/api/cdv-oscillator?symbol=&window=1800&bucket=60` — compute per-minute CDV delta and price change, normalize both to [-1, 1], return oscillator = CDV_norm - price_norm, plus divergence labels at key inflection points.
2. Frontend: new card "CDV Oscillator" with dual-axis Chart.js line chart — CDV series (purple), Price series (gold), and a zero-line oscillator (teal bars). Highlight divergence zones with red/green shading.
3. Data: uses existing `get_recent_trades` → no new DB tables needed.

## Signal Logic
- CDV new high + price flat/down → bearish divergence (hidden selling)
- CDV new low + price flat/up → bullish divergence (hidden buying)
- Oscillator bar > 0 = CDV leading price (bullish bias)
- Oscillator bar < 0 = price leading CDV (momentum unsupported)
