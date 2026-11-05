# ROOM SIX — Hyperliquid AI Trading Room

Independent read-only six-agent application. Runs on Railway from branch `feature/room-six-hyperliquid`. This branch is isolated from the existing aggressive trading bot.

## Deploy on Railway
1. Create Railway project → Deploy from GitHub → select `notbetterplacethanhome-netizen/TRADERBOT` and branch `feature/room-six-hyperliquid`.
2. Dockerfile builds FastAPI automatically. Railway reads `railway.json`.
3. Variables: `ROOM_SIX_TOKEN` = a long random secret (32+ characters); `HYPERLIQUID_NETWORK=mainnet`; `ROOM_SIX_DB=/app/data/room-six.db`.
4. Optional real model access: `ROOM_SIX_LLM_API_KEY`, `ROOM_SIX_LLM_MODEL`, `ROOM_SIX_LLM_BASE_URL=https://api.openai.com/v1`.
5. Mount persistent Railway volume at `/app/data` for SQLite history. Generate a Railway domain. Enter `ROOM_SIX_TOKEN` into the web UI.
6. Check `/api/health` (public endpoint).

## Scope
Responsive trading room with 6 role cards, read-only Hyperliquid market snapshots (BTC, ETH, SOL, HYPE), 2-hour momentum, order-book imbalance and rule-based baseline. API has independent model calls for Scout, Chart, Orderflow and News; Skeptic and Chief see preceding role responses. News abstains because there is no news source. Agent outputs saved to SQLite; no secret keys are sent to browsers. All results are advisory; real orders are **not implemented**. Deliberations default to WAIT without properly configured models. No claim of profitability.

## API
`GET /api/health` public; `GET /api/market/BTC`, `GET /api/agents/status`, `POST /api/agents/deliberate/BTC`, `GET /api/history` require `Authorization: Bearer <ROOM_SIX_TOKEN>`.

## Local
```sh
pip install -r requirements.txt
ROOM_SIX_TOKEN=use-a-long-random-secret uvicorn app:app --port 8000
```

## Limitations and security
This is a Railway-ready **initial implementation**, not a production trading bot. No exchange wallet, no live order execution, no live order history, no news feed, and no proof of strategy profitability. API calls to the configured LLM provider may cost money. The app uses a single worker and SQLite. A public domain exposes the static UI; use Railway-level access protection for a private deployment.
