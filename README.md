# ROOM SIX — Hyperliquid AI Trading Room

Independent **read-only** six-role analytical room. Runs on Railway from branch
`feature/room-six-hyperliquid`. Isolated from the aggressive trading bot.

**Работает без API-ключа.** Шесть ролей считаются формулами по реальным данным
биржи. Модель — опция: если она задана, роли спросят её вместо формулы.

## Deploy on Railway
1. Create Railway project → Deploy from GitHub → `notbetterplacethanhome-netizen/TRADERBOT`,
   branch `feature/room-six-hyperliquid`.
2. Dockerfile builds FastAPI automatically. Railway reads `railway.json`.
3. **Обязательные переменные:** `ROOM_SIX_TOKEN` = a long random secret (32+ chars);
   `HYPERLIQUID_NETWORK=mainnet`; `ROOM_SIX_DB=/app/data/room-six.db`.
4. **Необязательно** (модель вместо формул): `ROOM_SIX_LLM_API_KEY`,
   `ROOM_SIX_LLM_MODEL`, `ROOM_SIX_LLM_BASE_URL`. Без них роли работают формулами.
5. Mount a persistent Railway volume at `/app/data` for SQLite history.
6. Check `/api/health` (public endpoint).

## Роли

| Роль | Что считает |
|---|---|
| Scout | импульс за 2 часа (свечи 15м) |
| Chart | тренд: цена против средней за 20 свечей |
| Orderflow | перекос стакана (биды vs аски, топ-10) |
| News | воздерживается: ленты новостей нет |
| Skeptic | вето при расколе сигналов или высокой волатильности |
| Chief | агрегатор: большинство + правило рынка |

Пороги: `MOMENTUM_PCT=0.3`, `TREND_PCT=0.5`, `IMBALANCE=0.12`.

## Scope
Responsive trading room with 6 role cards, read-only Hyperliquid market snapshots
(12 самых ликвидных: BTC, ETH, HYPE, ZEC, SOL, NEAR, XRP, XMR, PUMP, TRUMP, LIT, WLD), 2-hour momentum, order-book imbalance and a rule-based
baseline. Agent outputs saved to SQLite; no secret keys are sent to browsers.
All results are advisory; real orders are **not implemented**.
No claim of profitability.

## API
`GET /api/health` public; `GET /api/market/<COIN>`, `GET /api/agents/status`,
`POST /api/agents/deliberate/<COIN>`, `GET /api/history` require
`Authorization: Bearer <ROOM_SIX_TOKEN>`.

## Local
```sh
pip install -r requirements.txt
ROOM_SIX_TOKEN=use-a-long-random-secret uvicorn app:app --port 8000
```

## Limitations and security
No exchange wallet, no live order execution, no live order history, no news feed,
and no proof of strategy profitability. If a model is configured, its calls may
cost money. Single worker and SQLite. A public domain exposes the static UI; use
Railway-level protection for a private deployment.
