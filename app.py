import os, json, time, secrets, asyncio, sqlite3
from pathlib import Path
from typing import Optional
import httpx
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app=FastAPI(title="ROOM SIX", docs_url=None, redoc_url=None)
ROOT=Path(__file__).resolve().parent
TOKEN=os.getenv("ROOM_SIX_TOKEN","")
DB=os.getenv("ROOM_SIX_DB","/tmp/room-six.db")
NETWORK=os.getenv("HYPERLIQUID_NETWORK","mainnet")
ENDPOINT="https://api.hyperliquid-testnet.xyz/info" if NETWORK=="testnet" else "https://api.hyperliquid.xyz/info"
COINS={"BTC","ETH","SOL","HYPE"}
ROLES=("Scout","Chart","Orderflow","News","Skeptic","Chief")
def auth(authorization):
    if not TOKEN: raise HTTPException(503,"ROOM_SIX_TOKEN must be configured")
    if not secrets.compare_digest(authorization or "","Bearer "+TOKEN): raise HTTPException(401,"Unauthorized")
def database():
    Path(DB).parent.mkdir(parents=True,exist_ok=True)
    conn=sqlite3.connect(DB,timeout=15)
    conn.execute("CREATE TABLE IF NOT EXISTS deliberations (id INTEGER PRIMARY KEY, ts INTEGER, coin TEXT, response TEXT)")
    return conn
async def info(payload):
    async with httpx.AsyncClient(timeout=12) as client:
        r=await client.post(ENDPOINT,json=payload)
        r.raise_for_status()
        return r.json()
def clean_coin(coin):
    coin=coin.upper()
    if coin not in COINS: raise HTTPException(422,"Unsupported coin")
    return coin
async def snapshot(coin):
    coin=clean_coin(coin)
    book,candles,mids=await asyncio.gather(
        info({"type":"l2Book","coin":coin}),
        info({"type":"candleSnapshot","req":{"coin":coin,"interval":"15m","startTime":int(time.time()*1000)-15*60*1000*80,"endTime":int(time.time()*1000)}}),
        info({"type":"allMids"})
    )
    levels=book.get("levels",[[],[]])
    def total(arr):return sum(float(x.get("sz",0)) for x in arr[:10])
    bid,ask=total(levels[0]),total(levels[1])
    close=[float(c["c"]) for c in candles if "c" in c]
    price=float(mids.get(coin,0))
    change=(close[-1]/close[-9]-1)*100 if len(close)>9 and close[-9]>0 else 0
    imbalance=(bid-ask)/(bid+ask) if bid+ask>0 else 0
    return {"coin":coin,"price":price,"change_2h_pct":round(change,4),"orderbook_imbalance":round(imbalance,4),"bid_top10":bid,"ask_top10":ask,"last_close":close[-1] if close else None,"sample_size":len(close),"time_ms":int(time.time()*1000),"network":NETWORK}
def baseline(m):
    if m["price"]<=0 or m["sample_size"]<12:return "WAIT"
    if m["change_2h_pct"]>0.3 and m["orderbook_imbalance"]>0.12:return "LONG"
    if m["change_2h_pct"]< -0.3 and m["orderbook_imbalance"]< -0.12:return "SHORT"
    return "WAIT"
async def call_llm(role, market, prior):
    key=os.getenv("ROOM_SIX_LLM_API_KEY","")
    model=os.getenv("ROOM_SIX_LLM_MODEL","")
    if not key or not model: return {"role":role,"vote":"WAIT","reason":"LLM is not configured","connected":False}
    root=os.getenv("ROOM_SIX_LLM_BASE_URL","https://api.openai.com/v1").rstrip("/")
    msg="You are "+role+" in an audit-focused crypto paper-analysis team. Only use supplied public market data. News: no news feed; abstain. Return ONLY JSON: {\\\"vote\\\":\\\"LONG|SHORT|WAIT\\\",\\\"reason\\\":\\\"brief explanation\\\"}. No promises or fictitious feeds. Chief must WAIT on ambiguity. No direct order authority."
    body={"model":model,"temperature":0,"messages":[{"role":"system","content":msg},{"role":"user","content":json.dumps({"market":market,"previous":prior},ensure_ascii=False)}]}
    try:
        async with httpx.AsyncClient(timeout=35) as cli:
            response=await cli.post(root+"/chat/completions",headers={"Authorization":"Bearer "+key},json=body)
            response.raise_for_status()
            content=response.json()["choices"][0]["message"]["content"]
        value=json.loads(content.strip().removeprefix("```json").removesuffix("```").strip())
        vote=value.get("vote","WAIT")
        if vote not in ("LONG","SHORT","WAIT"):vote="WAIT"
        return {"role":role,"vote":vote,"reason":str(value.get("reason",""))[:500],"connected":True}
    except Exception as e:
        return {"role":role,"vote":"WAIT","reason":"LLM unavailable: "+type(e).__name__,"connected":False}
@app.get("/",response_class=HTMLResponse)
async def home():
    return (ROOT/"index.html").read_text(encoding="utf-8")
@app.get("/api/health")
async def health():
    return {"ok":True,"mode":"READ_ONLY","exchange":NETWORK,"live_execution":False}
@app.get("/api/market/{coin}")
async def market(coin:str,authorization:Optional[str]=Header(None)):
    auth(authorization)
    try:return await snapshot(coin)
    except HTTPException:raise
    except Exception:raise HTTPException(503,"Hyperliquid market data unavailable")
@app.get("/api/agents/status")
async def status(authorization:Optional[str]=Header(None)):
    auth(authorization)
    return {"llm_enabled":bool(os.getenv("ROOM_SIX_LLM_API_KEY") and os.getenv("ROOM_SIX_LLM_MODEL")),"roles":ROLES,"mode":"ADVISORY_ONLY"}
@app.post("/api/agents/deliberate/{coin}")
async def deliberate(coin:str,authorization:Optional[str]=Header(None)):
    auth(authorization)
    try:m=await snapshot(coin)
    except HTTPException:raise
    except Exception:raise HTTPException(503,"Hyperliquid market data unavailable")
    first=await asyncio.gather(*(call_llm(r,m,[]) for r in ROLES[:4]))
    skeptic=await call_llm("Skeptic",m,first)
    chief=await call_llm("Chief",m,[*first,skeptic])
    votes=[x["vote"] for x in [*first,skeptic,chief] if x["role"]!="News" and x["connected"]]
    decision="WAIT"
    if len(votes)==5 and all(v==votes[0] for v in votes) and baseline(m)==votes[0]:
        decision=votes[0]
    data={"market":m,"agents":[*first,skeptic,chief],"baseline":baseline(m),"decision":decision,"advisory_only":True,"exchange_order_sent":False}
    with database() as con:con.execute("INSERT INTO deliberations(ts,coin,response) VALUES(?,?,?)",(int(time.time()),m["coin"],json.dumps(data)))
    return data
@app.get("/api/history")
async def history(authorization:Optional[str]=Header(None)):
    auth(authorization)
    with database() as con:
        rows=con.execute("SELECT ts,coin,response FROM deliberations ORDER BY id DESC LIMIT 20").fetchall()
    return [{"ts":r[0],"coin":r[1],"decision":json.loads(r[2])["decision"]} for r in rows]
