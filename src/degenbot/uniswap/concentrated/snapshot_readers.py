"""Concentrated-liquidity map values."""

from typing import TypedDict

from degenbot.uniswap.concentrated.types import BitmapAtWord, LiquidityAtTick


class LiquidityMap(TypedDict):
    """Bitmap and initialized tick values for a pool."""

    tick_bitmap: dict[int, BitmapAtWord]
    tick_data: dict[int, LiquidityAtTick]
