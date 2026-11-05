import os
os.environ.setdefault("ROOM_SIX_TOKEN","test-secret")
os.environ.setdefault("ROOM_SIX_DB","/tmp/room-six-tests.db")
from app import baseline, clean_coin, auth
from fastapi import HTTPException

def test_wait_without_market():
    assert baseline({"price":0,"sample_size":0,"change_2h_pct":2,"orderbook_imbalance":0.4})=="WAIT"

def test_long_confirmation():
    assert baseline({"price":100,"sample_size":70,"change_2h_pct":0.5,"orderbook_imbalance":0.2})=="LONG"

def test_short_confirmation():
    assert baseline({"price":100,"sample_size":70,"change_2h_pct":-0.5,"orderbook_imbalance":-0.2})=="SHORT"

def test_invalid_coin():
    try:clean_coin("BAD")
    except HTTPException as e:assert e.status_code==422
    else:assert False

def test_reject_wrong_token():
    try:auth("Bearer wrong")
    except HTTPException as e:assert e.status_code==401
    else:assert False
