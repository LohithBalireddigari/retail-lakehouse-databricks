"""Bronze layer: land raw files as-is, plus ingestion metadata."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import RAW_FORMATS
from .schemas import RAW_SCHEMAS


def add_ingestion_metadata(df: DataFrame, source: str) -> DataFrame:
    """Tag every row with when and from where it was ingested."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(source))
    )


def read_raw(spark: SparkSession, entity: str, path: str) -> DataFrame:
    """Batch-read one raw entity with its explicit string schema."""
    fmt = RAW_FORMATS[entity]
    reader = spark.read.schema(RAW_SCHEMAS[entity])
    if fmt == "csv":
        reader = reader.option("header", "true")
    df = reader.format(fmt).load(path)
    return add_ingestion_metadata(df, entity)


def read_raw_stream(spark: SparkSession, entity: str, path: str, schema_location: str) -> DataFrame:
    """Incrementally read new files with Databricks Auto Loader (cloudFiles).

    Only works on Databricks; local tests use :func:`read_raw`.
    """
    fmt = RAW_FORMATS[entity]
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", schema_location)
        .schema(RAW_SCHEMAS[entity])
    )
    if fmt == "csv":
        reader = reader.option("header", "true")
    return add_ingestion_metadata(reader.load(path), entity)
