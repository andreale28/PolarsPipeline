import polars as pl
import patito as pt


def validate_df(df: pl.DataFrame | pl.LazyFrame, schema: pt.Model) -> None:
    """
    A wrapper function of patito.Model.validate to validate the input DataFrame.

    Args:

        df (pl.DataFrame): The DataFrame to validate.
        schema (pt.Model): The patito schema to validate the DataFrame with.
    Raises:
        patito.exceptions.DataFrameValidationError: If the given dataframe does not match
        the given schema
    """
    if not isinstance(df, pl.DataFrame):
        df = df.collect(streaming=True)

    schema.validate(df)
