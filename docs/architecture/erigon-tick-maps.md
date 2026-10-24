# Complete tick maps from local Erigon

Pool initialization requires the Erigon `erigon_getTickMaps` method. Configure
the endpoint to expose the `erigon` namespace, for example with
`--http.api=eth,erigon`. Initialization loads one complete snapshot at a
canonical block, then applies live pool events incrementally.

## RPC contract

`erigon_getTickMaps` takes a list of pool descriptors and a block selector:

```json
[
  [
    {
      "address": "0x1111111111111111111111111111111111111111",
      "protocol": "uniswapV3",
      "tickSpacing": 60
    }
  ],
  {
    "blockHash": "0x2222222222222222222222222222222222222222222222222222222222222222",
    "requireCanonical": true
  }
]
```

Supported protocol tags are `uniswapV3`, `pancakeV3`, and `uniswapV4`.
For V4, `address` is the PoolManager and `poolId` is required. The descriptor
supplies the known contract layout and immutable tick spacing; the server
does not infer them from an address.

The response contains `chainId`, `blockNumber`, `blockHash`, and a `pools`
list in request order. Each pool includes its descriptor, `tick`,
`sqrtPriceX96`, `liquidity`, `protocolFee`, `lpFee`, and every initialized
tick in ascending order. Each tick carries `tick`, `liquidityGross`, and
`liquidityNet`. Chain and block numbers, price, and unsigned liquidity are
hex quantities. Signed net liquidity is a decimal string.

Erigon scans the complete valid bitmap range and reads initialized ticks
inside one state transaction. No bitmap-word or tick RPC requests cross the
network. Snapshots are complete or fail; there is no pagination or truncation.
One request accepts up to 64 pools; larger batches can share the same block hash.
The Rust client checks block and pool identity, ordered tick boundaries,
liquidity prefixes, zero total net liquidity, and active liquidity before
the builder registers the pool.

The bulk builder takes mutable pool state and ticks from this snapshot,
sets full (`Tracked`) coverage, and stamps both state clocks with the
snapshot block. It bypasses the database seed and attaches no lazy word
fetcher. Empty initialized-tick lists also have full coverage when the
snapshot's liquidity is zero.

Tracked pools still pass the existing verification lifecycle before becoming
live. Buffered events already included in a snapshot are ignored; later
events retain their order, including multiple events from the same block.
