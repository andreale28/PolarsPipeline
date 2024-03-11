from datetime import datetime
from typing import Optional

import polars as pl
import pyarrow as pa
import patito as pt
from patito import Model

PL_SCHEMA = {
    "_index": pl.String,
    "_type": pl.String,
    "_id": pl.String,
    "_score": pl.Int64,
    "_source": pl.Struct(
        [
            pl.Field("Contract", pl.String),
            pl.Field("Mac", pl.String),
            pl.Field("TotalDuration", pl.Int64),
            pl.Field("AppName", pl.String),
        ]
    ),
}

PA_SCHEMA = pa.schema(
    [
        pa.field("_index", pa.string()),
        pa.field("_type", pa.string()),
        pa.field("_id", pa.string()),
        pa.field("_score", pa.int64()),
        pa.field(
            "_source",
            pa.struct(
                [
                    pa.field("Contract", pa.string()),
                    pa.field("Mac", pa.string()),
                    pa.field("TotalDuration", pa.int64()),
                    pa.field("AppName", pa.string()),
                ]
            ),
        ),
    ]
)


class Output(Model):
    Contract: str = pt.Field(dtype=pl.String, unique=True)
    TVDuration: int = pt.Field(dtype=pl.Int64)
    ChildDuration: int = pt.Field(dtype=pl.Int64)
    SportDuration: int = pt.Field(dtype=pl.Int64)
    RelaxDuration: int = pt.Field(dtype=pl.Int64)
    MovieDuration: int = pt.Field(dtype=pl.Int64)
    RFM: int = pt.Field(dtype=pl.Int64)
    MostWatch: str = pt.Field(dtype=pl.String)
    is_current: bool = pt.Field(dtype=pl.Boolean)
    effective_time: datetime = pt.Field(dtype=pl.Datetime)
    end_time: Optional[datetime]
