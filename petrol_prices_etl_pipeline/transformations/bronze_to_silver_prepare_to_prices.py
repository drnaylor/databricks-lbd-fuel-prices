# Import modules
from pyspark import pipelines as dp
from pyspark.sql import Column
from pyspark.sql import functions as F, Window
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructType, StructField, DecimalType

from datetime import datetime


@dp.table(
  private=True,
  name="prepare_prices"
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
               # We expect a number that is three whole digits, so we take it down this way. We want to divide
               # by 10 if we have four digits to get to 3 digits, noting log10(1000) is 3 so we would want 10 out of this,
               # which is 10 ^ [log_10(1000) - 2] => 10 ^ (3 - 2) => 10 ^ 1 => 10
               .when(col > 1000.00, col / (F.power(10, F.floor(F.log10(col) - 2))))
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
            ).where(
                F.col("price_timestamp").isNotNull()
            )
    )

dp.create_streaming_table("silver.petrol_prices.prices")

dp.create_auto_cdc_flow(
    source="prepare_prices",
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
