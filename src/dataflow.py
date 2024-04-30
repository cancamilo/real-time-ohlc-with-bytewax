import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Any
import numpy as np
from typing_extensions import override

import websockets
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async
from bytewax.operators.window import EventClockConfig, TumblingWindow
from bytewax.operators import window as window_op
from bytewax.outputs import StatelessSinkPartition, DynamicSink

from src.date_utils import str2epoch, epoch2datetime


async def _ws_agen(product_id):
    url = "wss://ws-feed.exchange.coinbase.com"
    async with websockets.connect(url) as websocket:
        msg = json.dumps(
            {
                "type": "subscribe",
                "product_ids": [product_id],
                "channels": ["ticker"],
            }
        )
        await websocket.send(msg)
        # The first msg is just a confirmation that we have subscribed.
        await websocket.recv()

        while True:
            msg = await websocket.recv()
            yield json.loads(msg)

class CoinbasePartition(StatefulSourcePartition):
    def __init__(self, product_id):
        agen = _ws_agen(product_id)
        self._batcher = batch_async(agen, timedelta(seconds=0.5), 100)

    def next_batch(self):
        return next(self._batcher)

    def snapshot(self):
        return None

@dataclass
class CoinbaseSource(FixedPartitionedSource):
    product_ids: List[str]

    def list_parts(self):
        return self.product_ids

    def build_part(self, step_id, for_key, _resume_state):
        return CoinbasePartition(for_key)

@dataclass
class Ticker:
    product_id: str
    ts_unix: int
    price: float
    size: float


class StreamUtils:

    @staticmethod
    def get_event_time(ticker: Ticker) -> datetime:
        """
        This function instructs the event clock on how to retrieve the
        event's datetime from the input.
        """
        return epoch2datetime(ticker.ts_unix)

    @staticmethod
    def build_array() -> np.array:
        """_summary_

        Returns:
            np.array: _description_
        """

        return np.empty((0, 3))

    @staticmethod
    def acc_values(previous_data: np.array, ticker: Ticker) -> np.array:
        """
        This is the accumulator function, and outputs a numpy array of time and price
        """

        return np.insert(
            previous_data, 0, np.array((ticker.ts_unix, ticker.price, ticker.size)), 0
        )

    # compute OHLC for the window

    @staticmethod
    def calculate_features(ticker_data: Tuple[str, np.array]) -> Tuple[str, Dict]:
        """Aggregate trade data in window

        Args:
            ticker__data (Tuple[str, np.array]): product_id, data

        Returns:
            Tuple[str, Dict]: product_id, Dict with keys
                - time
                - open
                - high
                - low
                - close
                - volume
        """
        ticker, window_tuple = ticker_data
        # print("ticker", ticker)
        window, data = window_tuple
        # print("ticker window", window)
        # print("ticker data", data)

        ohlc = {
            "time": data[-1][0],
            "open": data[:, 1][-1],
            "high": np.amax(data[:, 1]),
            "low": np.amin(data[:, 1]),
            "close": data[:, 1][0],
            "volume": np.sum(data[:, 2]),
        }
        return (ticker, ohlc)

    @staticmethod
    def key_on_product(data: Dict) -> Tuple[str, Ticker]:
        """Transform input `data` into a Tuple[product_id, ticker_data]
        where `ticker_data` is a `Ticker` object.

        Args:
            data (Dict): _description_

        Returns:
            Tuple[str, Ticker]: _description_
        """

        ticker = Ticker(
            product_id=data["product_id"],
            ts_unix=str2epoch(data["time"]),
            price=data["price"],
            size=data["last_size"],
        )
        return (data["product_id"], ticker)


def get_dataflow(window_seconds: int = 5, return_stream=False) -> Dataflow:
    """Constructs and returns a ByteWax Dataflow

    Args:
        window_seconds (int)

    Returns:
        Dataflow:
    """

    init_flow = Dataflow("ticker_flow")

    socket_stream = op.input("input", init_flow, CoinbaseSource(["ETH-USD", "BTC-USD"]))

    # (ticker_data) -> (product_id, ticker_obj)
    keyed_stream = op.map("transform", socket_stream, StreamUtils.key_on_product)

    # Configure the `fold_window` operator to use the event time
    cc = EventClockConfig(
        StreamUtils.get_event_time, wait_for_system_duration=timedelta(seconds=10)
    )

    start_at = datetime.now(timezone.utc)
    start_at = start_at - timedelta(
        seconds=start_at.second, microseconds=start_at.microsecond
    )
    wc = TumblingWindow(align_to=start_at, length=timedelta(seconds=window_seconds))

    window_stream = window_op.fold_window(
        f"{window_seconds}_sec",
        keyed_stream,
        cc,
        wc,
        StreamUtils.build_array,
        StreamUtils.acc_values,
    )

    mapped_stream = op.map(
        "feature_mapper", window_stream, StreamUtils.calculate_features
    )

    op.output("out", mapped_stream, StdOutSink())

    # # compute technical-indicators
    # from src.technical_indicators import BollingerBands
    # flow.stateful_map(
    #     "technical_indicators",
    #     lambda: BollingerBands(3),
    #     BollingerBands.compute
    # )

    if return_stream:
        return init_flow, mapped_stream

    return init_flow
