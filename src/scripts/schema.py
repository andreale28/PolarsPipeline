import polars as pl
import pyarrow as pa

pl_schema = {
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

pa_schema = pa.schema(
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
