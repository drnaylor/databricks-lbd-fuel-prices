# Import modules
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructType, StructField, DecimalType

# Define the path to the source data
prices_file_path = f"/Volumes/bronze/petrol_prices/csv/prices"

def create_price_cols(type: str):
    return [
      StructField(f"forecourts.fuel_price.{type}", DecimalType(10,4), True),
      StructField(f"forecourts.price_change_effective.{type}", StringType(), True),
      StructField(f"forecourts.price_submission_timestamp.{type}", StringType(), True)
    ]

# Define the CSV definition that we want to pull into a bronze layer
prices_schema = StructType(
  [
    StructField("forecourt_update_timestamp", StringType(), True),
    StructField("mft.name", StringType(), True),
    StructField("forecourts.node_id", StringType(), True),
    StructField("forecourts.trading_name", StringType(), True),
    StructField("forecourts.brand_name", StringType(), True),
    StructField("forecourts.is_motorway_service_station", BooleanType(), True),
    StructField("forecourts.is_supermarket_service_station", BooleanType(), True),
    StructField("forecourts.public_phone_number", StringType(), True),
    StructField("forecourts.temporary_closure", StringType(), True),
    StructField("forecourts.permanent_closure", StringType(), True),
    StructField("forecourts.permanent_closure_date", StringType(), True),
    StructField("forecourts.location.postcode", StringType(), True),
    StructField("forecourts.location.address_line_1", StringType(), True),
    StructField("forecourts.location.address_line_2", StringType(), True),
    StructField("forecourts.location.city", StringType(), True),
    StructField("forecourts.location.county", StringType(), True),
    StructField("forecourts.location.country", StringType(), True),
    StructField("forecourts.location.latitude", DecimalType(15,10), True),
    StructField("forecourts.location.longitude", DecimalType(15,10), True),
  ] 
  # Order is important here...
  + create_price_cols("E5") 
  + create_price_cols("E10") 
  + create_price_cols("B7S")
  + create_price_cols("B7P")
  + create_price_cols("B10")
  + create_price_cols("HVO")
  # later columns are not important for us so we don't define them.
)

@dp.table(
  name="bronze.petrol_prices.prices_raw",
  comment="Raw data from the Petrol Prices API."
)
def prices_raw():
  def to_boolean(column_name: str) -> F.Column:
    return (
      F.when(F.lower(column_name) == "true", F.lit(True))
        .when(F.lower(column_name) == "false", F.lit(False))
        .otherwise(None)
    )

  def convert_to_timestamp(col) -> F.Column:
    return F.to_timestamp(
        F.regexp_extract(col, r"^[A-Za-z]{3} ([A-Za-z]{3} \d{2} \d{4} \d{2}:\d{2}:\d{2} GMT\+\d{4}).*$", 1), "MMM dd yyyy HH:mm:ss 'GMT'Z"
    )

  return (spark.readStream
    .format("cloudFiles")
    .schema(prices_schema)
    .option("header", "true")
    .option("cloudFiles.format", "csv")
    .load(prices_file_path)
    .select(
        F.col("forecourt_update_timestamp").alias("last_update_string"),
        convert_to_timestamp("forecourt_update_timestamp").alias("entry_timestamp"),
        F.col("`forecourts.node_id`").alias("forecourt_id"),
        F.col("`forecourts.trading_name`").alias("trading_name"),
        F.col("`forecourts.brand_name`").alias("brand_name"),
        F.col("`forecourts.is_motorway_service_station`").alias("motorway_service_station_flag"),
        F.col("`forecourts.is_supermarket_service_station`").alias("supermarket_flag"),
        F.col("`forecourts.public_phone_number`").alias("phone_number"),
        to_boolean("`forecourts.temporary_closure`").alias("temporary_closure"),
        to_boolean("`forecourts.permanent_closure`").alias("permanent_closure"),
        F.col("`forecourts.location.postcode`").alias("postcode"),
        F.col("`forecourts.location.address_line_1`").alias("address_line_1"),
        F.col("`forecourts.location.address_line_2`").alias("address_line_2"),
        F.col("`forecourts.location.city`").alias("city"),
        F.col("`forecourts.location.county`").alias("county"),
        F.col("`forecourts.location.country`").alias("country"),
        F.col("`forecourts.location.latitude`").alias("latitude"),
        F.col("`forecourts.location.longitude`").alias("longitude"),
        F.col("`forecourts.fuel_price.E5`").alias("E5"),
        convert_to_timestamp("`forecourts.price_change_effective.E5`").alias("E5_timestamp"),
        F.col("`forecourts.fuel_price.E10`").alias("E10"),
        convert_to_timestamp("`forecourts.price_change_effective.E10`").alias("E10_timestamp"),
        F.col("`forecourts.fuel_price.B7P`").alias("B7P"),
        convert_to_timestamp("`forecourts.price_change_effective.B7P`").alias("B7P_timestamp"),
        F.col("`forecourts.fuel_price.B7S`").alias("B7S"),
        convert_to_timestamp("`forecourts.price_change_effective.B7S`").alias("B7S_timestamp"),
        F.col("`forecourts.fuel_price.B10`").alias("B10"),
        convert_to_timestamp("`forecourts.price_change_effective.B10`").alias("B10_timestamp"),
        F.col("`forecourts.fuel_price.HVO`").alias("HVO"),
        convert_to_timestamp("`forecourts.price_change_effective.HVO`").alias("HVO_timestamp")
    )).distinct()


postcode_file_path = f"/Volumes/bronze/petrol_prices/csv/postcode"

postcode_schema = StructType(
  [
    StructField("id", IntegerType(), False),
    StructField("postcode", StringType(), False),
    StructField("latitude", DecimalType(15,10), True),
    StructField("longitude", DecimalType(15,10), True)
  ]
)

@dp.table(
  name="bronze.petrol_prices.postcodes",
  comment="Raw data from the Postcodes CSV."
)
def postcodes_raw():
  return (spark.readStream
    .format("cloudFiles")
    .schema(postcode_schema)
    .option("header", "true")
    .option("cloudFiles.format", "csv")
    .load(postcode_file_path)
    .select(
        F.current_timestamp().alias("ingestion_time"),
        F.col("postcode"),
        F.col("latitude"),
        F.col("longitude")
    ))
  
fuel_types_file_path = f"/Volumes/bronze/petrol_prices/csv/fuel_types/fuel_types.csv"

fuel_types_schema = StructType(
  [
    StructField("fuel_type_code", StringType(), False),
    StructField("fuel_type_description", StringType(), False),
  ]
)

@dp.table(
  name="bronze.petrol_prices.fuel_types",
  comment="Fuel types from the fuel types CSV"
)
def fuel_types_raw():
  return (spark.read
    .format("csv")
    .schema(fuel_types_schema)
    .option("header", "true")
    .load(fuel_types_file_path)
    .select(
        F.col("fuel_type_code"),
        F.col("fuel_type_description")
    ))
  
