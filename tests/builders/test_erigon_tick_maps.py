"""The configured Python driver builds complete maps through the native RPC path."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from degenbot.crypto import function_selector
from tests.helpers.verdict_probe import run

_POOL = "0x" + "aa" * 20
_TOKEN0 = "0x" + "11" * 20
_TOKEN1 = "0x" + "22" * 20
_FACTORY = "0x" + "bb" * 20
_HASH = "0x" + "cc" * 32


@pytest.mark.parametrize("supported", [True, False], ids=["complete", "unsupported"])
def test_python_driver_requires_complete_erigon_tick_maps(*, supported: bool) -> None:
    seen: list[dict[str, Any]] = []
    forbidden: list[str] = []
    immutable = {
        "0x" + function_selector(signature).hex(): f"0x{value:064x}"
        for signature, value in {
            "factory()": int(_FACTORY, 16),
            "token0()": int(_TOKEN0, 16),
            "token1()": int(_TOKEN1, 16),
            "fee()": 3000,
            "tickSpacing()": 60,
        }.items()
    }
    header = {
        "hash": _HASH,
        "parentHash": "0x" + "00" * 32,
        "sha3Uncles": "0x" + "00" * 32,
        "miner": "0x" + "00" * 20,
        "stateRoot": "0x" + "00" * 32,
        "transactionsRoot": "0x" + "00" * 32,
        "receiptsRoot": "0x" + "00" * 32,
        "logsBloom": "0x" + "00" * 256,
        "difficulty": "0x0",
        "number": "0x64",
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x0",
        "timestamp": "0x6553f100",
        "extraData": "0x",
        "mixHash": "0x" + "00" * 32,
        "nonce": "0x" + "00" * 8,
        "transactions": [],
        "uncles": [],
        "baseFeePerGas": "0x1",
    }
    snapshot = {
        "chainId": "0x1",
        "blockNumber": "0x64",
        "blockHash": _HASH,
        "pools": [
            {
                "address": _POOL,
                "protocol": "uniswapV3",
                "tickSpacing": 60,
                "tick": 0,
                "sqrtPriceX96": hex(2**96),
                "liquidity": "0x64",
                "protocolFee": 0,
                "lpFee": 3000,
                "ticks": [
                    {"tick": -60000, "liquidityGross": "0x64", "liquidityNet": "100"},
                    {"tick": 120000, "liquidityGross": "0x64", "liquidityNet": "-100"},
                ],
            }
        ],
    }

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            seen.append(request)
            method, params = request["method"], request.get("params", [])
            error = None
            if method == "eth_chainId":
                result = "0x1"
            elif method == "eth_blockNumber":
                result = "0x64"
            elif method == "eth_getBlockByNumber":
                result = header
            elif method == "erigon_getTickMaps":
                result = snapshot
                if not supported:
                    error = {"code": -32601, "message": "erigon_getTickMaps unavailable"}
            elif method == "eth_call" and params[0]["to"].lower() == _POOL:
                calldata = params[0].get("input", params[0].get("data", ""))
                result = immutable.get(calldata)
                if result is None:
                    forbidden.append(calldata)
                    error = {"code": -32601, "message": f"incremental getter forbidden: {calldata}"}
            else:
                result = None
                forbidden.append(method)
                error = {"code": -32601, "message": f"unexpected RPC: {method}"}
            response = {"jsonrpc": "2.0", "id": request["id"]}
            response["error" if error else "result"] = error or result
            payload = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        child = run(
            f"""
from degenbot._ffi import Bot, PoolTickCoverage
from degenbot._ffi.provider import AlloyProvider
bot = Bot(chain_id=1)
provider = AlloyProvider("http://127.0.0.1:{server.server_port}", max_retries=0, chain_id=1)
bot.attach_construction_io(provider, None)
for address in ({_TOKEN0!r}, {_TOKEN1!r}):
    bot.register_token(address, "Token", "T", 18, 1)
try:
    pool_id, *_ = bot.build_v3_pool({_POOL!r}, block=100)
except RuntimeError as error:
    assert not {supported!r}, str(error)
    assert type(error) is RuntimeError
    assert "erigon_getTickMaps" in str(error), str(error)
else:
    assert {supported!r}, "unsupported endpoint silently fell back"
    view = bot.get_pool(pool_id).concentrated_liquidity()
    assert view.tick_data == {{-60000: (100, 100, 100), 120000: (100, -100, 100)}}
    assert set(view.tick_bitmap) == {{-4, 7}}
    assert view.coverage == PoolTickCoverage.Tracked
    assert (view.sqrt_price_x96, view.tick, view.liquidity) == (2**96, 0, 100)
    assert (view.update_block, view.tick_data_block) == (100, 100)
""",
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    assert child.returncode == 0, child.stdout + child.stderr
    assert not forbidden, forbidden
    bulk = [request for request in seen if request["method"] == "erigon_getTickMaps"]
    assert len(bulk) == 1
    assert bulk[0]["params"] == [
        [{"address": _POOL, "protocol": "uniswapV3", "tickSpacing": 60}],
        {"blockHash": _HASH, "requireCanonical": True},
    ]
