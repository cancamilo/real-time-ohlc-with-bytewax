from typing import List, Any
import pandas as pd
import streamlit as st
from typing_extensions import override

from bytewax.dataflow import Dataflow
from bytewax.testing import run_main
from bytewax import operators as op
from bytewax.outputs import StatelessSinkPartition, DynamicSink

from src.plot import get_candlestick_plot
from src.date_utils import epoch2datetime
from src.dataflow import get_dataflow

WINDOW_SECONDS = 30

st.set_page_config(layout="wide")
st.title(f"ETH/USD OHLC data every {WINDOW_SECONDS} seconds")
# st.header('Lines represent Bollinger Bands')

# here we store the data our Stream processing outputs
df = pd.DataFrame()


class OutputSink(StatelessSinkPartition[Any]):
    def __init__(self) -> None:
        super().__init__()
        self.placeholder = st.empty()

    @override
    def write_batch(self, items: List[Any]) -> None:

        dt, data = items[0]
        
        # add 'date' key with datetime
        data['date'] = epoch2datetime(data['time'])

        # append one row with the latest observation `data`
        global df
        # df = df.append(data, ignore_index=True)
        df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)

        with self.placeholder.container():
            p = get_candlestick_plot(df, WINDOW_SECONDS)
            st.bokeh_chart(p, use_container_width=True)

class AggregateSink(DynamicSink[Any]):
    @override
    def build(
        self, _step_id: str, _worker_index: int, _worker_count: int
    ) -> OutputSink:
        return OutputSink()


init_flow, windowed_flow = get_dataflow(window_seconds=WINDOW_SECONDS, return_stream=True)
op.output("stateless_out", windowed_flow, AggregateSink())
run_main(init_flow)
