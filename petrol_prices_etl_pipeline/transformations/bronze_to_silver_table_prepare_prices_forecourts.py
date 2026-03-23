# Import modules
from pyspark import pipelines as dp
from pyspark.sql import Column
from pyspark.sql import functions as F, Window
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructType, StructField, DecimalType

from datetime import datetime

def clean_postcode(col: str | Column):
    colfunc = F.col(col) if isinstance(col, str) else col
    return F.when(
        F.regexp_count(F.trim(colfunc), F.lit(r"\s+")) == 0, 
        F.upper(F.concat_ws(" ", F.substring(colfunc, 1, F.length(colfunc) - 3), F.substring(colfunc, F.length(colfunc) - 3, 3)))
    ).otherwise(F.upper(colfunc))

@dp.table(name="silver.petrol_prices.cdc_data")
def prepare_data_for_cdc():
    """
    Private table to perform transformations before injecting them into the CDC flow.
    """
    return (
        spark.readStream
            .option("skipChangeCommits", "true")
            .table("bronze.petrol_prices.prices_raw")
            .select(
                F.col("entry_timestamp"),
                F.col("trading_name"),
                F.col("brand_name"),
                F.col("motorway_service_station_flag"),
                F.col("supermarket_flag"),
                F.col("phone_number"),
                F.col("temporary_closure"),
                F.col("permanent_closure"),
                clean_postcode("postcode").alias("postcode"),
                F.col("address_line_1"),
                F.col("address_line_2"),
                F.col("city"),
                F.col("county"),
                F.col("country"),
                F.col("latitude"),
                F.col("longitude"),
                F.col("E5"),
                F.col("E5_timestamp"),
                F.col("E10"),
                F.col("E10_timestamp"),
                F.col("B7P"),
                F.col("B7P_timestamp"),
                F.col("B7S"),
                F.col("B7S_timestamp"),
                F.col("B10"),
                F.col("B10_timestamp"),
                F.col("HVO"),
                F.col("HVO_timestamp")
            ).withColumn(
                "forecourt_id",
                # The Forecourt ID changes for some forecourts, so we generate our own based on postcode and
                # trading name.
                F.xxhash64(F.col("postcode"), F.col("trading_name"))
            )
    )

    

