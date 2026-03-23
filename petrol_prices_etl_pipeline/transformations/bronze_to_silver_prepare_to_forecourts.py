# Import modules
from pyspark import pipelines as dp
from pyspark.sql import Column
from pyspark.sql import functions as F, Window
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructType, StructField, DecimalType

from datetime import datetime

@dp.table(
    private=True,
    name="prepare_forecourts"
)
@dp.expect_or_drop("no_qa_entries", """
                   LOWER(trading_name) NOT LIKE '%preprod%' AND
                   LOWER(trading_name) NOT LIKE '%-new' AND
                   (LOWER(brand_name) NOT LIKE '%pre-prod%' OR brand_name IS NULL)
                   """)
def forecourts_cleaned():
    return spark.readStream.table("silver.petrol_prices.cdc_data").select(
        F.col("entry_timestamp"),
        F.col("forecourt_id"),
        F.coalesce(F.col("trading_name"), F.col("brand_name")).alias("trading_name"),
        F.col("brand_name"),
        F.col("motorway_service_station_flag"),
        F.col("supermarket_flag"),
        F.col("phone_number"),
        F.coalesce(F.col("temporary_closure"), F.lit(False)).alias("temporary_closure"),
        F.coalesce(F.col("permanent_closure"), F.lit(False)).alias("permanent_closure"),
        F.col("postcode"),
        F.col("address_line_1"),
        F.col("address_line_2"),
        F.col("city"),
        F.col("county"),
        F.col("country"),
        F.col("latitude").alias("reported_latitude"),
        F.col("longitude").alias("reported_longitude")
    )


dp.create_streaming_table("silver.petrol_prices.forecourts")

dp.create_auto_cdc_flow(
    source="prepare_forecourts",
    target="silver.petrol_prices.forecourts",
    keys=["forecourt_id"],
    sequence_by="entry_timestamp",
    stored_as_scd_type=2
)
