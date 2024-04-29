from datetime import datetime
from typing import Annotated

import pandas as pd
from pydantic import BaseModel, ConfigDict, PlainSerializer


class Strategy(BaseModel):
    id: str
    namespace: str


class Allocation(BaseModel):
    strategy: Strategy
    alloc: float


class StrategyPerformanceParams(BaseModel):
    allocations: list[Allocation]
    start: datetime
    end: datetime


PandasSeries = Annotated[pd.Series, PlainSerializer(lambda series: series.to_dict())]


class StrategyPerformance(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    params: StrategyPerformanceParams
    performance: PandasSeries
    underwater: PandasSeries
    metrics: dict
