# Import modules
from pyspark import pipelines as dp
from pyspark.sql import Column
from pyspark.sql import functions as F, Window
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructType, StructField, DecimalType

def silver_table(clean_price):
    lag_window = Window.partitionBy("forecourt_id").orderBy(F.desc_nulls_first("entry_timestamp"))

    # We need to clean the data
    return (
        spark.read.table("bronze.petrol_prices.prices_raw")
            .select(
                F.col("entry_timestamp"),
                F.lag("entry_timestamp").over(lag_window).alias("entry_close_timestamp"),
                F.col("name"),
                F.col("forecourt_id"),
                F.col("trading_name"),
                F.col("brand_name"),
                F.col("motorway_service_station_flag"),
                F.col("supermarket_flag"),
                F.col("phone_number"),
                F.coalesce("temporary_closure", F.lit(False)).alias("temporary_closure"),
                F.coalesce("permanent_closure", F.lit(False)).alias("permanent_closure"),
                F.col("postcode"),
                F.col("address_line_1"),
                F.col("address_line_2"),
                F.col("city"),
                F.col("county"),
                F.col("country"),
                F.col("latitude"),
                F.col("longitude"),
                clean_price("super_unleaded"),
                clean_price("unleaded"),
                clean_price("premium_diesel"),
                clean_price("diesel"),
                clean_price("biodiesel"),
                clean_price("hydrogen")
            )
    )

@dp.table(
  name="silver.petrol_prices.prices",
  comment="Cleaned data from the Petrol Prices API."
)
# They must have a fuel price, or closed
@dp.expect_or_drop("closed_or_price", """
                   unleaded IS NOT NULL OR 
                      super_unleaded IS NOT NULL OR
                      diesel IS NOT NULL OR
                      premium_diesel IS NOT NULL OR
                      biodiesel IS NOT NULL OR
                      hydrogen IS NOT NULL OR
                      temporary_closure <> TRUE OR
                      permanent_closure <> TRUE
                   """)
@dp.expect_or_drop("no_qa_entries", """
                   name NOT IN ('TESTQA', 'Test AS') AND
                   LOWER(trading_name) NOT LIKE '%PreProd%' AND
                   LOWER(trading_name) NOT LIKE '%-new'
                   """)
@dp.expect_or_fail("no_outrageous_fuel_prices", """
                   (super_unleaded IS NULL OR super_unleaded BETWEEN 50.00 AND 500.00) AND
                   (unleaded IS NULL OR unleaded BETWEEN 50.00 AND 500.00) AND
                   (diesel IS NULL OR diesel BETWEEN 50.00 AND 500.00) AND
                   (premium_diesel IS NULL OR premium_diesel BETWEEN 50.00 AND 500.00) AND
                   (biodiesel IS NULL OR biodiesel BETWEEN 50.00 AND 500.00)
                   """)
def prices_cleaned():
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
    
    return silver_table(clean_price)


@dp.table(
  name="silver.petrol_prices.uncleaned_prices",
  comment="Not clean data from the Petrol Prices API."
)
def prices_uncleaned():    
    return silver_table(lambda s: F.col(s))


@dp.table(
    name="silver.petrol_prices.postcodes",
    comment="Postcode data from the Petrol Prices API."
)
def postcode():
    return (
        spark.read.table("bronze.petrol_prices.postcodes")
            .select(
                F.monotonically_increasing_id().alias("entry_id"),
                "ingestion_time",
                "postcode",
                "longitude",
                "latitude"
            )
)
