
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional, Sequence


def read_delta_table(
    spark: SparkSession,
    table_name: str,
    columns: Optional[Sequence[str]] = None,
    filters: Optional[dict] = None
) -> DataFrame:
    """
    Read a Delta table with optional column selection
    and equality filters.

    Example filters:
        {"ClientCode": "H225", "BenefitYear": 2026}
    """
    df = spark.table(table_name)

    if columns:
        df = df.select(*columns)

    if filters:
        for column_name, value in filters.items():
            if value is None:
                df = df.filter(F.col(column_name).isNull())
            else:
                df = df.filter(F.col(column_name) == value)

    return df
