# Import modules
from pyspark import pipelines as dp
from pyspark.sql import Column
from pyspark.sql import functions as F, Window
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructType, StructField, DecimalType

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
def prices_cleaned():
    def clean_price(col_name: str) -> Column:
        col = F.col(col_name)
        return (
            F.when(col < "50.00", col * 100.0) # we have pounds, we want pence
               .otherwise(col)
               .alias(col_name)
        )

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