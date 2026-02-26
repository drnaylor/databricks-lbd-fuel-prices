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

@dp.table(private=True)
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
                F.xxhash64(clean_postcode("postcode"), F.col("trading_name"))
            )
    )


dp.create_streaming_table("silver.petrol_prices.cdc_data")

dp.create_auto_cdc_flow(
    source="prepare_data_for_cdc",
    target="silver.petrol_prices.cdc_data",
    keys=["forecourt_id"],
    sequence_by="entry_timestamp",
    stored_as_scd_type=2,
    column_list=[
        F.col("entry_timestamp"),
        F.col("forecourt_id"),
        F.col("trading_name"),
        F.col("brand_name"),
        F.col("motorway_service_station_flag"),
        F.col("supermarket_flag"),
        F.col("phone_number"),
        F.col("temporary_closure"),
        F.col("permanent_closure"),
        F.col("postcode"),
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
    ]
)

@dp.table(
  name="silver.petrol_prices.forecourts",
  comment="Cleaned forecourt data from the Petrol Prices API."
)
@dp.expect_or_drop("no_qa_entries", """
                   LOWER(trading_name) NOT LIKE '%preprod%' AND
                   LOWER(trading_name) NOT LIKE '%-new' AND
                   (LOWER(brand_name) NOT LIKE '%pre-prod%' OR brand_name IS NULL)
                   """)
def forecourts_cleaned():
    return spark.readStream.option("skipChangeCommits", "true").table("silver.petrol_prices.cdc_data").select(
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
        F.col("longitude").alias("reported_longitude"),
        F.col("__START_AT"),
        F.col("__END_AT")
    )

@dp.table(
  private=True,
  name="silver.petrol_prices.prepare_prices"
)
@dp.expect_or_fail("no_outrageous_prices", """
        `price` IS NULL OR `price` BETWEEN 50.00 AND 500.00
        """)
def prices():
    def clean_price(col_name: str) -> Column:
        col = F.col(col_name)
        return (
            F.when(col < 0.5, F.lit(None)) # We're not reporting it
               .when(col < 2.50, col * 100.0) # we have pounds, we want pence
               .when(col < 50.00, col * 10.0) # Dimes...
               .when(col > 1000.00, col / F.floor(F.log10(col) - 3)) # We expect a number that is three whole digits, so we take it down this way
               .when(col > 500.00, col / 10.0)
               .otherwise(col)
               .alias(col_name)
        )

    def combine_fuel_cols(first):
        return (
            F.named_struct(
                F.lit("original_price"),
                F.col(first),
                F.lit("price"),
                clean_price(first), 
                F.lit("price_timestamp"),
                F.col(f"{first}_timestamp")
            ).alias(first)
        )

    return (
        spark.readStream
            .option("skipChangeCommits", "true")
            .table("silver.petrol_prices.cdc_data")
            .select(
                F.col("forecourt_id"),
                F.col("postcode"),
                combine_fuel_cols("E5"),
                combine_fuel_cols("E10"),
                combine_fuel_cols("B7S"),
                combine_fuel_cols("B7P"),
                combine_fuel_cols("B10"),
                combine_fuel_cols("HVO")
            ).unpivot(
                ids=["forecourt_id", "postcode"],
                values=[
                    "E5",
                    "E10",
                    "B7S",
                    "B7P",
                    "B10",
                    "HVO"
                ],
                variableColumnName="fuel_type_code",
                valueColumnName="price_and_timestamp"
            ).select(
                F.col("forecourt_id"),
                F.col("postcode"),
                F.col("fuel_type_code"),
                F.col("price_and_timestamp.original_price").alias("original_price"),
                F.col("price_and_timestamp.price").alias("price"),
                F.col("price_and_timestamp.price_timestamp").alias("price_timestamp")
            )
    )

dp.create_streaming_table("silver.petrol_prices.prices")

dp.create_auto_cdc_flow(
    source="silver.petrol_prices.prepare_prices",
    target="silver.petrol_prices.prices",
    keys=["forecourt_id", "fuel_type_code"],
    sequence_by="price_timestamp",
    stored_as_scd_type=2,
    column_list=[
        F.col("forecourt_id"),
        F.col("postcode"),
        F.col("fuel_type_code"),
        F.col("original_price"),
        F.col("price"),
        F.col("price_timestamp")
    ]
)

@dp.table(
    name="silver.petrol_prices.postcodes",
    comment="Postcode data from the Petrol Prices API."
)
def postcode():
    return (
        spark.read.table("bronze.petrol_prices.postcodes")
            .select(
                "ingestion_time",
                "postcode",
                "longitude",
                "latitude"
            )
)
    
@dp.materialized_view(
    name="silver.petrol_prices.fuel_types",
    comment="Fuel types from the Petrol Prices API."
)
def fuel_types():
  return spark.read.table("bronze.petrol_prices.fuel_types")

    

