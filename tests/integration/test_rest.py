import asyncio
from pprint import pprint

import requests
import json

import websockets

target = '://localhost:5000'
HTTP_TARGET = 'http' + target
WS_TARGET = 'ws' + target


def post(endpoint, data):
    return requests.post(HTTP_TARGET + endpoint, json.dumps(data))


def test_fetch_ticker():
    response = post("/kraken/fetchOHLCV", {"symbol": "ETH/BTC"})
    assert (response.status_code == 200)
    return response


def test_parallel_fetch_ticker():
    response = post("/parallel/kraken/fetchOHLCV",
                    [{"symbol": "LTC/BTC"}, {"symbol": "XMR/BTC"}, {"symbol": "ETH/BTC"}])
    assert (response.status_code == 200)
    return response


async def test_ws_fetch():
    async with websockets.connect(WS_TARGET + '/ws/fetch/kraken') as websocket:

        await websocket.send(json.dumps([['fetchOHLCV', {"symbol": "LTC/BTC"}], ['fetchOrderBook', {"symbol": "LTC/BTC"}]]))
        while True:
            result = await websocket.recv()
            pprint(json.loads(result))
