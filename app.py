"""ROOM SIX — шестиролевой аналитический совет по Hyperliquid.

Только чтение: ордера не отправляются. Решение — совещательное.

ШЕСТЬ РОЛЕЙ РАБОТАЮТ БЕЗ ИИ. Каждая роль считает своё по реальным данным
биржи (импульс, тренд, стакан). Если задан ключ модели — роль спросит модель
вместо формулы; если нет — считает формула. Без ключа приложение полностью
работоспособно и бесплатно.
"""
import os, json, re, time, secrets, asyncio, sqlite3, statistics
from pathlib import Path
from typing import Optional
import httpx
import websockets
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title="ROOM SIX", docs_url=None, redoc_url=None)
ROOT = Path(__file__).resolve().parent
TOKEN = os.getenv("ROOM_SIX_TOKEN", "")
DB = os.getenv("ROOM_SIX_DB", "/tmp/room-six.db")
NETWORK = os.getenv("HYPERLIQUID_NETWORK", "mainnet")
ENDPOINT = ("https://api.hyperliquid-testnet.xyz/info" if NETWORK == "testnet"
            else "https://api.hyperliquid.xyz/info")
# WebSocket — основной источник данных (05.11.26, слово владельца). REST остаётся
# только как страховка на случай, если поток не поднялся.
WS_URL = ("wss://api.hyperliquid-testnet.xyz/ws" if NETWORK == "testnet"
          else "wss://api.hyperliquid.xyz/ws")
# 12 самых ликвидных монет Hyperliquid (05.11.26, по объёму за сутки).
# Было четыре — владелец спросил «не мало ли». Отбор: топ-12 по объёму 24ч,
# чтобы стакан был плотный и сигналы не по пустым книгам.
COIN_LIST = ("BTC", "ETH", "HYPE", "ZEC", "SOL", "NEAR",
             "XRP", "XMR", "PUMP", "TRUMP", "LIT", "WLD")
COINS = set(COIN_LIST)
LOOP_SECONDS = 90          # как часто смотрим рынок (бесплатно)
COUNCIL_COOLDOWN = 600     # не чаще раза в 10 минут зовём модель по монете
ROLES = ("Scout", "Chart", "Orderflow", "News", "Skeptic", "Chief")

# Пороги ролей. Меняются осознанно, как и всё в торговле.
MOMENTUM_PCT = 0.3      # импульс за 2 часа, % от цены
TREND_PCT = 0.5         # цена над средней за 20 свечей, %
IMBALANCE = 0.12        # перекос стакана (0..1)


def auth(authorization):
    if not TOKEN:
        raise HTTPException(503, "ROOM_SIX_TOKEN must be configured")
    if not secrets.compare_digest(authorization or "", "Bearer " + TOKEN):
        raise HTTPException(401, "Unauthorized")


def database():
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB, timeout=15)
    conn.execute("""CREATE TABLE IF NOT EXISTS deliberations
                    (id INTEGER PRIMARY KEY, ts INTEGER, coin TEXT, response TEXT)""")
    return conn


async def info(payload):
    async with httpx.AsyncClient(timeout=12) as client:
        r = await client.post(ENDPOINT, json=payload)
        r.raise_for_status()
        return r.json()


def clean_coin(coin):
    coin = coin.upper()
    if coin not in COINS:
        raise HTTPException(422, "Unsupported coin")
    return coin


class MarketFeed:
    """Живой поток Hyperliquid: цены, стакан и свечи по WebSocket.

    Опрос REST каждые 90 секунд — это десятки запросов в минуту и данные с
    задержкой. Поток отдаёт изменения мгновенно и одним соединением.
    """

    def __init__(self):
        self.prices = {}
        self.books = {}
        self.candles = {c: [] for c in COIN_LIST}
        self.ready = False
        self.last_ms = 0
        self.reconnects = 0
        self.messages = 0

    async def start(self):
        while True:
            try:
                await self._run()
            except Exception:
                self.reconnects += 1
                self.ready = False
                await asyncio.sleep(3)

    async def _fill_history(self):
        """Однократно добираем историю свечей: поток отдаёт только текущую."""
        for coin in COIN_LIST:
            try:
                arr = await info({"type": "candleSnapshot", "req": {
                    "coin": coin, "interval": "15m",
                    "startTime": int(time.time() * 1000) - 15 * 60 * 1000 * 80}})
                self.candles[coin] = arr[-80:]
            except Exception:
                pass

    async def _run(self):
        await self._fill_history()
        async with websockets.connect(WS_URL, ping_interval=20, open_timeout=20) as ws:
            for coin in COIN_LIST:
                await ws.send(json.dumps({"method": "subscribe",
                                          "subscription": {"type": "l2Book", "coin": coin}}))
                await ws.send(json.dumps({"method": "subscribe",
                                          "subscription": {"type": "candle",
                                                           "coin": coin, "interval": "15m"}}))
            await ws.send(json.dumps({"method": "subscribe",
                                      "subscription": {"type": "allMids"}}))
            self.ready = True
            async for raw in ws:
                try:
                    self._apply(json.loads(raw))
                    self.messages += 1
                    self.last_ms = int(time.time() * 1000)
                except Exception:
                    continue

    def _apply(self, d):
        ch = d.get("channel")
        data = d.get("data")
        if ch == "allMids" and isinstance(data, dict):
            self.prices.update(data.get("mids") or {})
        elif ch == "l2Book" and isinstance(data, dict):
            coin = data.get("coin")
            if coin:
                self.books[coin] = data
        elif ch == "candle" and data:
            c = data[0] if isinstance(data, list) else data
            coin = c.get("s") or c.get("coin")
            if not coin:
                return
            arr = self.candles.setdefault(coin, [])
            t_ms = int(c.get("t", 0))
            if arr and int(arr[-1].get("t", 0)) == t_ms:
                arr[-1] = c
            elif not arr or int(arr[-1].get("t", 0)) < t_ms:
                arr.append(c)
                if len(arr) > 200:
                    del arr[:-200]

    def fresh(self, max_age=90):
        return self.ready and self.last_ms and (time.time() * 1000 - self.last_ms) < max_age * 1000


FEED = MarketFeed()


def _snapshot_from_feed(coin):
    book = FEED.books.get(coin) or {}
    levels = book.get("levels", [[], []])

    def total(arr):
        return sum(float(x.get("sz", 0)) for x in arr[:10])

    bid, ask = total(levels[0]), total(levels[1])
    price = float(FEED.prices.get(coin, 0) or 0)
    candles = FEED.candles.get(coin) or []
    close = [float(c["c"]) for c in candles if "c" in c]
    if not price and close:
        price = close[-1]
    change = (close[-1] / close[-9] - 1) * 100 if len(close) > 9 and close[-9] > 0 else 0
    imbalance = (bid - ask) / (bid + ask) if bid + ask > 0 else 0
    trend = 0.0
    if len(close) >= 20:
        sma = sum(close[-20:]) / 20
        trend = (price / sma - 1) * 100 if sma > 0 else 0.0
    vol = 0.0
    if len(close) > 6:
        rets = [(close[k] / close[k - 1] - 1) * 100 for k in range(len(close) - 5, len(close))]
        vol = statistics.pstdev(rets) if len(rets) > 1 else 0.0
    return {"coin": coin, "price": price, "change_2h_pct": round(change, 4),
            "orderbook_imbalance": round(imbalance, 4), "bid_top10": bid, "ask_top10": ask,
            "trend_pct": round(trend, 4), "volatility_pct": round(vol, 4),
            "last_close": close[-1] if close else None, "sample_size": len(close),
            "time_ms": int(time.time() * 1000), "network": NETWORK, "source": "websocket"}


async def snapshot(coin):
    """Снимок из потока; если поток молчит — идём в REST."""
    coin = clean_coin(coin)
    if FEED.fresh():
        snap = _snapshot_from_feed(coin)
        if snap["price"] > 0 and snap["sample_size"] >= 12:
            return snap
    return await snapshot_rest(coin)


async def snapshot_rest(coin):
    coin = clean_coin(coin)
    book, candles, mids = await asyncio.gather(
        info({"type": "l2Book", "coin": coin}),
        info({"type": "candleSnapshot", "req": {
            "coin": coin, "interval": "15m",
            "startTime": int(time.time() * 1000) - 15 * 60 * 1000 * 80,
            "endTime": int(time.time() * 1000)}}),
        info({"type": "allMids"}))
    levels = book.get("levels", [[], []])

    def total(arr):
        return sum(float(x.get("sz", 0)) for x in arr[:10])

    bid, ask = total(levels[0]), total(levels[1])
    close = [float(c["c"]) for c in candles if "c" in c]
    price = float(mids.get(coin, 0))
    change = (close[-1] / close[-9] - 1) * 100 if len(close) > 9 and close[-9] > 0 else 0
    imbalance = (bid - ask) / (bid + ask) if bid + ask > 0 else 0
    trend = 0.0
    if len(close) >= 20:
        sma = sum(close[-20:]) / 20
        trend = (price / sma - 1) * 100 if sma > 0 else 0.0
    vol = 0.0
    if len(close) > 6:
        rets = [(close[k] / close[k - 1] - 1) * 100 for k in range(len(close) - 5, len(close))]
        vol = statistics.pstdev(rets) if len(rets) > 1 else 0.0
    return {"coin": coin, "price": price, "change_2h_pct": round(change, 4),
            "orderbook_imbalance": round(imbalance, 4), "bid_top10": bid, "ask_top10": ask,
            "trend_pct": round(trend, 4), "volatility_pct": round(vol, 4),
            "last_close": close[-1] if close else None, "sample_size": len(close),
            "time_ms": int(time.time() * 1000), "network": NETWORK, "source": "rest"}


def baseline(m):
    """Правило рынка: импульс И стакан должны смотреть в одну сторону."""
    if m["price"] <= 0 or m["sample_size"] < 12:
        return "WAIT"
    if m["change_2h_pct"] > MOMENTUM_PCT and m["orderbook_imbalance"] > IMBALANCE:
        return "LONG"
    if m["change_2h_pct"] < -MOMENTUM_PCT and m["orderbook_imbalance"] < -IMBALANCE:
        return "SHORT"
    return "WAIT"


# ---------------------------------------------------------------- роли без ИИ
def role_vote(role, m, prior):
    """Детерминированное мнение роли. Без ключей, без сети, мгновенно."""
    chg = m["change_2h_pct"]
    imb = m["orderbook_imbalance"]
    trend = m.get("trend_pct", 0.0)
    vol = m.get("volatility_pct", 0.0)

    def out(vote, reason):
        return {"role": role, "vote": vote, "reason": reason, "connected": True, "source": "rule"}

    if role == "Scout":
        if chg > MOMENTUM_PCT:
            return out("LONG", f"импульс {chg:+.2f}% за 2 часа — движение вверх")
        if chg < -MOMENTUM_PCT:
            return out("SHORT", f"импульс {chg:+.2f}% за 2 часа — движение вниз")
        return out("WAIT", f"импульса нет ({chg:+.2f}% за 2 часа)")

    if role == "Chart":
        if trend > TREND_PCT:
            return out("LONG", f"цена на {trend:+.2f}% выше средней за 20 свечей")
        if trend < -TREND_PCT:
            return out("SHORT", f"цена на {trend:+.2f}% ниже средней за 20 свечей")
        return out("WAIT", f"цена у средней ({trend:+.2f}%) — тренда нет")

    if role == "Orderflow":
        if imb > IMBALANCE:
            return out("LONG", f"перекос стакана {imb:+.1%} в сторону покупок")
        if imb < -IMBALANCE:
            return out("SHORT", f"перекос стакана {imb:+.1%} в сторону продаж")
        return out("WAIT", f"стакан ровный ({imb:+.1%})")

    if role == "News":
        return out("WAIT", "ленты новостей нет — роль воздерживается")

    # Скептик и Шеф смотрят на предыдущие ответы.
    seen = [v for v in prior if v.get("role") != "News" and v.get("connected")]
    longs = sum(1 for v in seen if v["vote"] == "LONG")
    shorts = sum(1 for v in seen if v["vote"] == "SHORT")
    waits = sum(1 for v in seen if v["vote"] == "WAIT")

    if role == "Skeptic":
        if longs and shorts:
            return out("WAIT", f"сигналы противоречат: {longs} за рост, {shorts} за падение")
        if vol > 1.5:
            return out("WAIT", f"волатильность высокая ({vol:.2f}%) — вход рискованный")
        if not longs and not shorts:
            return out("WAIT", "все воздержались — подтверждать нечего")
        side = "LONG" if longs else "SHORT"
        return out(side, f"противоречий нет, {longs + shorts} голосов подтверждают {side}")

    if role == "Chief":
        base = baseline(m)
        if longs and shorts:
            return out("WAIT", "совет расколот — решение не принимается")
        if longs > shorts and base == "LONG":
            return out("LONG", f"большинство за рост ({longs}), правило рынка LONG")
        if shorts > longs and base == "SHORT":
            return out("SHORT", f"большинство за падение ({shorts}), правило рынка SHORT")
        if longs + shorts == 0:
            return out("WAIT", "совет не нашёл сигнала")
        return out("WAIT", "большинство и правило рынка не совпали")

    return out("WAIT", "роль не определена")


def normalize_base(url):
    """Приводим адрес API к правильному виду.

    05.11.26 владелец указал https://platform.deepseek.com — это ВЕБ-САЙТ, а не
    API: запрос вернул 405 Method Not Allowed, и все шесть ролей молча ушли на
    формулы. Та же беда с api.deepseek.com без /v1. Чиним сами, чтобы одна
    опечатка не выключала половину приложения.
    """
    u = (url or "").strip().rstrip("/")
    if not u:
        return "https://api.openai.com/v1"
    if "platform.deepseek.com" in u:
        return "https://api.deepseek.com/v1"
    if u.endswith("/chat/completions"):
        u = u[: -len("/chat/completions")]
    if u in ("https://api.deepseek.com", "http://api.deepseek.com"):
        return "https://api.deepseek.com/v1"
    return u


# ---------------------------------------------------------------- роли через ИИ
async def call_llm(role, market, prior):
    """Модель как ОПЦИЯ. Если ключ не задан — молча уходим на формулы."""
    key = os.getenv("ROOM_SIX_LLM_API_KEY", "")
    model = os.getenv("ROOM_SIX_LLM_MODEL", "")
    if not key or not model:
        return role_vote(role, market, prior)
    root = normalize_base(os.getenv("ROOM_SIX_LLM_BASE_URL", "https://api.openai.com/v1"))
    msg = ("You are " + role + " in an audit-focused crypto paper-analysis team. "
           "Use ONLY supplied public market data. News has no feed and MUST abstain. "
           "Return one JSON object with keys vote (LONG, SHORT, WAIT) and reason. "
           "Do not invent feeds, prices or executions. Chief must WAIT on ambiguity. "
           "Your output is advisory only.")
    # max_tokens обязателен: «думающие» модели тратят весь бюджет на рассуждение
    # и возвращают ПУСТОЙ content (проверено на deepseek-v4-pro: finish=length,
    # content='', весь ответ ушёл в reasoning_content).
    body = {"model": model, "temperature": 0, "max_tokens": 900,
            "messages": [{"role": "system", "content": msg},
                         {"role": "user", "content": json.dumps(
                             {"market": market, "previous": prior}, ensure_ascii=False)}]}
    try:
        async with httpx.AsyncClient(timeout=60) as cli:
            response = await cli.post(root + "/chat/completions",
                                      headers={"Authorization": "Bearer " + key}, json=body)
            response.raise_for_status()
            message = response.json()["choices"][0]["message"]
            # Обычная модель кладёт ответ в content, думающая — в reasoning_content.
            content = (message.get("content") or "").strip()
            if not content:
                content = (message.get("reasoning_content") or "").strip()
        # Вытаскиваем ПЕРВЫЙ JSON-объект: модели любят обрамлять его словами.
        match = re.search(r"\{.*\}", content, re.S)
        if not match:
            raise ValueError("no JSON in model answer")
        value = json.loads(match.group(0))
        vote = value.get("vote", "WAIT")
        if vote not in ("LONG", "SHORT", "WAIT"):
            vote = "WAIT"
        return {"role": role, "vote": vote, "reason": str(value.get("reason", ""))[:500],
                "connected": True, "source": "model"}
    except Exception as exc:
        # Модель недоступна или ответила мусором — НЕ теряем анализ.
        # Причину ВИДНО в ответе: молчаливый откат уже один раз спрятал
        # поломку (05.11.26: модель возвращала пустой content, все роли
        # отвечали формулами, и понять это было нечем).
        fallback = role_vote(role, market, prior)
        fallback["model_error"] = f"{type(exc).__name__}: {str(exc)[:180]}"
        fallback["model_model"] = model
        fallback["model_base"] = root
        return fallback


def decide(market, agents):
    """Решение: Шеф решает, правило рынка подтверждает."""
    chief = next((a for a in agents if a["role"] == "Chief"), None)
    if chief and chief["vote"] in ("LONG", "SHORT") and baseline(market) == chief["vote"]:
        return chief["vote"]
    return "WAIT"


# ---------------------------------------------------------------- автономный цикл
# Владелец 05.11.26: «чтобы я не тыкал токен и не нажимал собрать совет, пусть
# ждёт идеального совета». Значит: приложение само смотрит рынок по кругу, а
# модель зовём ТОЛЬКО когда правило рынка нашло повод. Иначе каждый круг стоил
# бы 24 запроса к модели впустую.
STATE = {"markets": {}, "council": {}, "updated_ms": 0, "rounds": 0,
         "signals": 0, "last_signal_ms": 0, "events": [], "next_round_ms": 0}


def add_event(text, kind="info"):
    """Строка журнала: по ней видно, что бот живой и что он делал."""
    STATE["events"].insert(0, {"ts": int(time.time() * 1000), "text": text, "kind": kind})
    del STATE["events"][40:]
_state_mtime = {}


async def run_council(coin: str) -> dict:
    """Полный совет по монете: шесть ролей + правило рынка."""
    m = await snapshot(coin)
    first = await asyncio.gather(*(call_llm(r, m, []) for r in ROLES[:4]))
    skeptic = await call_llm("Skeptic", m, first)
    chief = await call_llm("Chief", m, [*first, skeptic])
    agents = [*first, skeptic, chief]
    decision = decide(m, agents)
    data = {"market": m, "agents": agents, "baseline": baseline(m),
            "decision": decision, "advisory_only": True,
            "exchange_order_sent": False, "ts": int(time.time() * 1000)}
    with database() as con:
        con.execute("INSERT INTO deliberations(ts,coin,response) VALUES(?,?,?)",
                    (int(time.time()), m["coin"], json.dumps(data)))
    return data


async def watch_loop():
    """Крутится сам: рынок смотрим всегда, модель зовём только на сигнал."""
    await asyncio.sleep(3)
    while True:
        STATE["rounds"] += 1
        rnd = STATE["rounds"]
        add_event(f"круг {rnd}: смотрю рынок")
        found = []
        for coin in COIN_LIST:
            try:
                m = await snapshot(coin)
                STATE["markets"][coin] = m
                STATE["updated_ms"] = int(time.time() * 1000)
                base = baseline(m)
                if base == "WAIT":
                    continue
                found.append(f"{coin} {base}")
                # Повод есть — зовём совет, но не чаще раза в COOLDOWN.
                now = time.time()
                if now - _state_mtime.get(coin, 0) < COUNCIL_COOLDOWN:
                    continue
                _state_mtime[coin] = now
                data = await run_council(coin)
                STATE["council"][coin] = data
                if data["decision"] in ("LONG", "SHORT"):
                    STATE["signals"] += 1
                    STATE["last_signal_ms"] = data["ts"]
                    add_event(f"СИГНАЛ {data['decision']} {coin}: совет и правило сошлись", "signal")
                    await maybe_open(coin, data["decision"], m, "совет и правило сошлись")
                else:
                    add_event(f"{coin}: правило {base}, но совет воздержался", "wait")
            except Exception as exc:
                add_event(f"{coin}: сбой данных ({type(exc).__name__})", "err")
                continue
        # Стопы и трейлинг по открытым позициям — каждый круг.
        for coin, why in check_stops(STATE["markets"]):
            try:
                await maybe_close(coin, STATE["markets"].get(coin) or {}, why)
            except Exception as exc:
                add_event(f"{coin}: закрытие сбоем ({type(exc).__name__})", "err")

        if found:
            add_event("повод есть: " + ", ".join(found))
        else:
            add_event(f"круг {rnd}: повода нет — жду")
        STATE["next_round_ms"] = int((time.time() + LOOP_SECONDS) * 1000)
        await asyncio.sleep(LOOP_SECONDS)


@app.on_event("startup")
async def _start_watch():
    asyncio.create_task(FEED.start())
    asyncio.create_task(watch_loop())


# ---------------------------------------------------------------- исполнитель
# Владелец 05.11.26: «добавь чтобы он мог открывать и закрывать ордера».
#
# ПО УМОЛЧАНИЮ ВЫКЛЮЧЕНО. ROOM_SIX_EXECUTE:
#   off  - ордера не отправляются вовсе (по умолчанию);
#   dry  - бот РЕШАЕТ и пишет в журнал, что отправил бы, но на биржу не идёт;
#   live - настоящие ордера через API-кошелёк.
#
# Ключ API-кошелька умеет ТОРГОВАТЬ, но НЕ ВЫВОДИТЬ деньги — главный кошелёк
# на сервере не хранится. Это правило из CLAUDE.md, и оно не обсуждается.
EXECUTE = os.getenv("ROOM_SIX_EXECUTE", "off").strip().lower()
MAX_TRADE_USD = float(os.getenv("ROOM_SIX_MAX_TRADE_USD", "10"))
MAX_POSITIONS = int(os.getenv("ROOM_SIX_MAX_POSITIONS", "2"))
STOP_PCT = float(os.getenv("ROOM_SIX_STOP_PCT", "6"))
TRAIL_AFTER_PCT = float(os.getenv("ROOM_SIX_TRAIL_AFTER_PCT", "12"))
TRAIL_STOP_PCT = float(os.getenv("ROOM_SIX_TRAIL_STOP_PCT", "6"))
DAILY_LOSS_USD = float(os.getenv("ROOM_SIX_DAILY_LOSS_USD", "5"))

POSITIONS = {}          # coin -> {side, entry, size, stop, trail, opened_ms, dry}
DAY = {"date": None, "realized": 0.0, "closed": 0}


def _roll_day():
    today = time.strftime("%Y-%m-%d", time.gmtime())
    if DAY["date"] != today:
        DAY.update({"date": today, "realized": 0.0, "closed": 0})


def _exchange():
    """Ленивый SDK: без ключа приложение работает как раньше, без ордеров."""
    from eth_account import Account
    from hyperliquid.exchange import Exchange
    key = os.getenv("ROOM_SIX_HL_KEY", "").strip()
    acct = os.getenv("ROOM_SIX_HL_ACCOUNT", "").strip()
    if not key or not acct:
        raise RuntimeError("ROOM_SIX_HL_KEY / ROOM_SIX_HL_ACCOUNT не заданы")
    base = ("https://api.hyperliquid-testnet.xyz" if NETWORK == "testnet"
            else "https://api.hyperliquid.xyz")
    return Exchange(Account.from_key(key), base_url=base, account_address=acct)


def _sz_for(coin, price, usd):
    """Размер в монетах, округлённый вниз до шага монеты."""
    if price <= 0:
        return 0.0
    raw = usd / price
    try:
        from hyperliquid.utils import constants  # noqa: F401
    except Exception:
        pass
    # Простое округление: 4 знака для дешёвых, 3 для дорогих.
    return round(raw, 4) if raw < 100 else round(raw, 3)


async def order_market(coin, is_buy, price, usd, reduce_only=False):
    """Отправка рыночного ордера. Возвращает словарь с результатом."""
    sz = _sz_for(coin, price, usd)
    if sz <= 0:
        return {"ok": False, "error": "размер вышел нулевым"}
    dry = EXECUTE != "live"
    if EXECUTE == "off":
        return {"ok": False, "error": "исполнение выключено (ROOM_SIX_EXECUTE=off)"}
    if dry:
        return {"ok": True, "dry": True, "coin": coin, "is_buy": is_buy, "sz": sz,
                "note": "dry-run: ордер не отправлен"}
    try:
        ex = _exchange()
        # Рыночная заявка = лимит со проскальзыванием 1% и tif Ioc.
        slip = 1.01 if is_buy else 0.99
        res = ex.order(coin, is_buy, sz, round(price * slip, 6),
                       {"limit": {"tif": "Ioc"}}, reduce_only=reduce_only)
        ok = bool((res or {}).get("status") == "ok")
        return {"ok": ok, "raw": res, "coin": coin, "is_buy": is_buy, "sz": sz}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:200]}"}


async def maybe_open(coin, decision, market, reason):
    """Открыть позицию по решению совета, если исполнитель разрешает."""
    if EXECUTE == "off":
        return None
    _roll_day()
    if decision not in ("LONG", "SHORT"):
        return None
    if coin in POSITIONS:
        return None
    if len(POSITIONS) >= MAX_POSITIONS:
        add_event(f"{coin}: мест нет ({len(POSITIONS)}/{MAX_POSITIONS})", "wait")
        return None
    if DAY["realized"] <= -abs(DAILY_LOSS_USD):
        add_event(f"дневной лимит убытка {DAY['realized']:.2f}$ — торговля до завтра", "err")
        return None
    price = float(market.get("price") or 0)
    if price <= 0:
        return None
    is_buy = decision == "LONG"
    res = await order_market(coin, is_buy, price, MAX_TRADE_USD)
    if not res.get("ok"):
        add_event(f"{coin}: ордер НЕ отправлен — {res.get('error')}", "err")
        return None
    stop = price * (1 - STOP_PCT / 100) if is_buy else price * (1 + STOP_PCT / 100)
    POSITIONS[coin] = {"coin": coin, "side": decision, "entry": price,
                       "size": res.get("sz"), "stop": stop, "trail": None,
                       "opened_ms": int(time.time() * 1000), "dry": bool(res.get("dry")),
                       "reason": reason[:200]}
    add_event(f"{'[ПРИМЕР] ' if res.get('dry') else ''}ОТКРЫТ {decision} {coin} "
              f"${MAX_TRADE_USD:g} по {price:g}, стоп {stop:g}", "signal")
    return POSITIONS[coin]


async def maybe_close(coin, market, why):
    """Закрыть позицию. Стоп, трейлинг или рука владельца."""
    pos = POSITIONS.get(coin)
    if not pos:
        return None
    price = float(market.get("price") or 0)
    if price <= 0:
        return None
    is_buy = pos["side"] == "SHORT"   # закрытие шорта = покупка
    res = await order_market(coin, is_buy, price, MAX_TRADE_USD, reduce_only=True)
    if not res.get("ok"):
        add_event(f"{coin}: закрыть не вышло — {res.get('error')}", "err")
        return None
    move = (price - pos["entry"]) / pos["entry"] * 100
    pnl = (move if pos["side"] == "LONG" else -move) / 100 * MAX_TRADE_USD
    DAY["realized"] += pnl
    DAY["closed"] += 1
    POSITIONS.pop(coin, None)
    add_event(f"{'[ПРИМЕР] ' if pos.get('dry') else ''}ЗАКРЫТ {pos['side']} {coin} "
              f"по {price:g} ({pnl:+.2f}$), причина: {why}", "signal" if pnl >= 0 else "err")
    return {"coin": coin, "pnl": pnl, "why": why}


def check_stops(markets):
    """Стоп и трейлинг: вызывается каждый круг."""
    todo = []
    for coin, pos in list(POSITIONS.items()):
        m = markets.get(coin)
        if not m:
            continue
        price = float(m.get("price") or 0)
        if price <= 0:
            continue
        move = (price - pos["entry"]) / pos["entry"] * 100
        eff = move if pos["side"] == "LONG" else -move
        # стоп
        hit = (pos["side"] == "LONG" and price <= pos["stop"]) or \
              (pos["side"] == "SHORT" and price >= pos["stop"])
        if hit:
            todo.append((coin, "стоп"))
            continue
        # трейлинг после +TRAIL_AFTER_PCT
        if eff >= TRAIL_AFTER_PCT and not pos.get("trail"):
            trail = price * (1 - TRAIL_STOP_PCT / 100) if pos["side"] == "LONG" \
                    else price * (1 + TRAIL_STOP_PCT / 100)
            pos["trail"] = trail
            pos["stop"] = trail
            add_event(f"{coin}: прибыль {eff:+.1f}% — трейлинг подтянут до {trail:g}", "signal")
    return todo


# ---------------------------------------------------------------- API
@app.get("/", response_class=HTMLResponse)
async def home():
    return (ROOT / "index.html").read_text(encoding="utf-8")


@app.get("/api/latest")
async def latest():
    """Публично: последние посчитанные данные. Токен не нужен — это ЧТЕНИЕ.

    Владелец 05.11.26: «просто заходить в бота». Экран не должен требовать
    ввода ключа: он показывает то, что сервер уже посчитал сам.
    """
    return {"markets": STATE["markets"], "council": STATE["council"],
            "updated_ms": STATE["updated_ms"], "rounds": STATE["rounds"],
            "signals": STATE["signals"], "last_signal_ms": STATE["last_signal_ms"],
            "loop_seconds": LOOP_SECONDS, "exchange": NETWORK,
            "events": STATE["events"], "next_round_ms": STATE["next_round_ms"],
            "execute": EXECUTE, "positions": POSITIONS,
            "max_positions": MAX_POSITIONS, "max_trade_usd": MAX_TRADE_USD,
            "feed": {"ready": FEED.ready, "messages": FEED.messages,
                     "reconnects": FEED.reconnects, "last_ms": FEED.last_ms,
                     "fresh": FEED.fresh()},
            "model_enabled": bool(os.getenv("ROOM_SIX_LLM_API_KEY")
                                  and os.getenv("ROOM_SIX_LLM_MODEL"))}


@app.get("/api/positions")
async def positions():
    """Открытые позиции. Публично — это ЧТЕНИЕ."""
    _roll_day()
    return {"execute": EXECUTE, "max_trade_usd": MAX_TRADE_USD,
            "max_positions": MAX_POSITIONS, "stop_pct": STOP_PCT,
            "trail_after_pct": TRAIL_AFTER_PCT, "positions": POSITIONS,
            "day": DAY}


@app.post("/api/close/{coin}")
async def close_coin(coin: str, authorization: Optional[str] = Header(None)):
    """Закрыть позицию рукой владельца. Требует токен."""
    auth(authorization)
    coin = clean_coin(coin)
    if coin not in POSITIONS:
        raise HTTPException(404, "открытой позиции нет")
    m = STATE["markets"].get(coin) or await snapshot(coin)
    res = await maybe_close(coin, m, "закрыто владельцем")
    if not res:
        raise HTTPException(503, "закрыть не вышло — смотри журнал")
    return res


@app.get("/api/health")
async def health():
    return {"ok": True, "mode": "READ_ONLY", "exchange": NETWORK,
            "live_execution": False,
            "model_required": False,
            "model_enabled": bool(os.getenv("ROOM_SIX_LLM_API_KEY")
                                  and os.getenv("ROOM_SIX_LLM_MODEL"))}


@app.get("/api/market/{coin}")
async def market(coin: str, authorization: Optional[str] = Header(None)):
    auth(authorization)
    try:
        return await snapshot(coin)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(503, "Hyperliquid market data unavailable")


@app.get("/api/agents/status")
async def status(authorization: Optional[str] = Header(None)):
    auth(authorization)
    return {"llm_enabled": bool(os.getenv("ROOM_SIX_LLM_API_KEY")
                                and os.getenv("ROOM_SIX_LLM_MODEL")),
            "roles": ROLES, "mode": "ADVISORY_ONLY",
            "works_without_model": True}


@app.post("/api/agents/deliberate/{coin}")
async def deliberate(coin: str, authorization: Optional[str] = Header(None)):
    """Собрать совет вручную (кнопкой или curl). Экран его больше не требует."""
    auth(authorization)
    clean_coin(coin)
    try:
        return await run_council(coin)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(503, "Hyperliquid market data unavailable")


@app.get("/api/history")
async def history(authorization: Optional[str] = Header(None)):
    auth(authorization)
    with database() as con:
        rows = con.execute("SELECT ts,coin,response FROM deliberations "
                           "ORDER BY id DESC LIMIT 20").fetchall()
    return [{"ts": r[0], "coin": r[1], "decision": json.loads(r[2])["decision"]} for r in rows]
