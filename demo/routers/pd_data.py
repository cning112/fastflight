import dataclasses
import logging
from io import BytesIO
from typing import Annotated

import pandas as pd
from fastapi import APIRouter, Body, Depends, Query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pd_data")


def read_csv_to_df(data: Annotated[bytes, Body(...)], index_col: str):
    df = pd.read_csv(BytesIO(data)).set_index(index_col)
    return df


@dataclasses.dataclass
class CsvPayloadParams:
    df: Annotated[pd.DataFrame, Depends(read_csv_to_df)]
    y_column: str
    x_columns: Annotated[list[str], Query()]


@router.post("/csv/{filename}")
async def from_csv(filename: str, payload: Annotated[CsvPayloadParams, Depends()]):
    logger.info(f"csv {filename=}")
    logger.info(f"df length = {len(payload.df)}")
    logger.info(f"df index= {payload.df.index.name}")
    logger.info(f"y_column = {payload.y_column}")
    logger.info(f"x_columns = {payload.x_columns}")
    return {
        "filename": filename,
        "y_column": payload.y_column,
        "x_columns": payload.x_columns,
        "df.index": payload.df.index.name,
        "len(df)": len(payload.df),
    }
