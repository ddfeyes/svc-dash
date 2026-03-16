#!/bin/bash
# Auto-deploy: rebuild Docker + verify endpoints after every code change
# Run via git post-commit hook or cron

set -euo pipefail

LOG="/tmp/svc-dash-deploy.log"
COMPOSE_DIR="$HOME/svc-dash"
BACKEND_URL="http://localhost:8766/api"

echo "$(date '+%Y-%m-%d %H:%M:%S') — Deploy started" | tee -a "$LOG"

cd "$COMPOSE_DIR"

# 1. Rebuild backend (frontend is nginx, rarely changes)
echo "Rebuilding backend..." | tee -a "$LOG"
docker compose build backend >> "$LOG" 2>&1

# 2. Restart
echo "Restarting containers..." | tee -a "$LOG"
docker compose up -d >> "$LOG" 2>&1

# 3. Wait for healthy
echo "Waiting for health check..." | tee -a "$LOG"
for i in $(seq 1 30); do
  if curl -sf "$BACKEND_URL/health" > /dev/null 2>&1; then
    echo "Backend healthy after ${i}s" | tee -a "$LOG"
    break
  fi
  sleep 1
done

# 4. Verify ALL endpoints — the critical part
FAILED=0
PASSED=0
TOTAL=0

verify() {
  local name="$1"
  local url="$2"
  TOTAL=$((TOTAL + 1))
  
  RESPONSE=$(curl -sf -w "\n%{http_code}" "$url" 2>/dev/null || echo "CURL_FAILED")
  HTTP_CODE=$(echo "$RESPONSE" | tail -1)
  BODY=$(echo "$RESPONSE" | sed '$d')
  
  if [ "$HTTP_CODE" = "200" ] && echo "$BODY" | grep -q '"status":"ok"'; then
    PASSED=$((PASSED + 1))
    echo "  ✅ $name ($HTTP_CODE)" | tee -a "$LOG"
  else
    FAILED=$((FAILED + 1))
    echo "  ❌ $name ($HTTP_CODE)" | tee -a "$LOG"
  fi
}

echo "Verifying endpoints..." | tee -a "$LOG"
SYM="BANANAS31USDT"

verify "health" "$BACKEND_URL/health"
verify "symbols" "$BACKEND_URL/symbols"
verify "oi-history" "$BACKEND_URL/oi/history?symbol=$SYM&limit=5"
verify "funding-history" "$BACKEND_URL/funding/history?symbol=$SYM&limit=5"
verify "funding-momentum" "$BACKEND_URL/funding-momentum?symbol=$SYM"
verify "oi-weighted-price" "$BACKEND_URL/oi-weighted-price?symbol=$SYM"
verify "correlations-heatmap" "$BACKEND_URL/correlations/heatmap"
verify "ob-walls" "$BACKEND_URL/ob-walls?symbol=$SYM"
verify "realized-vol-bands" "$BACKEND_URL/realized-volatility-bands?symbol=$SYM"
verify "top-movers" "$BACKEND_URL/top-movers"
verify "ws-stats" "$BACKEND_URL/ws-stats"
verify "trade-size-percentiles" "$BACKEND_URL/trade-size-percentiles?symbol=$SYM"
verify "liquidation-heatmap" "$BACKEND_URL/liquidation-heatmap?symbol=$SYM"
verify "spread-history" "$BACKEND_URL/spread-history?symbol=$SYM&limit=5"
verify "vwap-deviation" "$BACKEND_URL/vwap-deviation?symbol=$SYM"
verify "vpin" "$BACKEND_URL/vpin?symbol=$SYM"
verify "market-regime" "$BACKEND_URL/market-regime?symbol=$SYM"
verify "aggressor-ratio" "$BACKEND_URL/aggressor-ratio?symbol=$SYM"
verify "tape-speed" "$BACKEND_URL/tape-speed?symbol=$SYM"

echo "" | tee -a "$LOG"
echo "Result: $PASSED/$TOTAL passed, $FAILED failed" | tee -a "$LOG"
echo "$(date '+%Y-%m-%d %H:%M:%S') — Deploy finished" | tee -a "$LOG"

if [ $FAILED -gt 0 ]; then
  echo "⚠️ DEPLOY HAS FAILURES — fix before declaring done" | tee -a "$LOG"
  exit 1
fi

echo "✅ All endpoints verified" | tee -a "$LOG"
exit 0
