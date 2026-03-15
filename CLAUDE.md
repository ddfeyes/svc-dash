# svc-dash — Claude Code Instructions

## Product Goal
Pattern research dashboard. NOT a data display tool — a tool to understand WHY an asset moves.
Key questions: What phase is the asset in? What precedes moves? Who controls price?

## Project
Crypto trading dashboard for BANANAS31USDT, COSUSDT, DEXEUSDT, LYNUSDT.
FastAPI + asyncio + SQLite backend. TradingView Lightweight Charts + Chart.js frontend.
Docker on :8765 (frontend) / :8766 (backend).
Secrets: ~/.lain-secrets/.env

## Product Spec
See: docs/superpowers/specs/2026-03-15-product-respec.md

## Key Indicators (implement correctly)
- CVD via taker side (not approximations)
- OI delta per 1-min candle
- Funding rate extremes + history
- Liquidation heatmap (cluster detection)
- Orderbook imbalance top-10 levels
- Large trades >$10k with direction
- Phase detector (accumulation/distribution/markup/markdown/ranging)

## Data Storage
- SQLite, 30+ days history
- 1-min OHLCV aggregated from trades
- OB snapshots every 10s
- Every trade stored

## Superpowers Skills
Skills dir: ~/.agents/skills/superpowers/
ALWAYS use Skill tool before starting any task:
- brainstorming/ — before any new feature
- test-driven-development/ — write tests first
- executing-plans/ — follow the plan strictly

## Coding Rules
- Write tests before implementation (TDD)
- Conventional commits (feat:, fix:, chore:, refactor:, test:)
- Do NOT run docker compose build
- Black + mypy before push
- Network: Binance/Bybit APIs only
- dangerous permissions: on

## GitHub
https://github.com/ddfeyes/svc-dash
