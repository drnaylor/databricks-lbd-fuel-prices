# Import modules
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructType, StructField, DecimalType

# Define the path to the source data
prices_file_path = f"/Volumes/bronze/petrol_prices/csv/prices"

# Define the CSV definition that we want to pull into a bronze layer
prices_schema = StructType(
  [
    StructField("latest_update_timestamp", StringType(), True),
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
    StructField("forecourts.fuel_price.E5", DecimalType(10,4), True),
    StructField("forecourts.fuel_price.E10", DecimalType(10,4), True),
    StructField("forecourts.fuel_price.B7P", DecimalType(10,4), True),
    StructField("forecourts.fuel_price.B7S", DecimalType(10,4), True),
    StructField("forecourts.fuel_price.B10", DecimalType(10,4), True),
    StructField("forecourts.fuel_price.HV0", DecimalType(10,4), True)
  ]
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

  return (spark.readStream
    .format("cloudFiles")
    .schema(prices_schema)
    .option("header", "true")
    .option("cloudFiles.format", "csv")
    .load(prices_file_path)
    .select(
        F.col("latest_update_timestamp").alias("last_update_string"),
        F.to_timestamp(
          F.regexp_extract("latest_update_timestamp", r"^[A-Za-z]{3} ([A-Za-z]{3} \d{2} \d{4} \d{2}:\d{2}:\d{2} GMT\+\d{4}).*$", 1), "MMM dd yyyy HH:mm:ss 'GMT'Z"
        ).alias("entry_timestamp"),
        F.col("`mft.name`").alias("name"),
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
        F.col("`forecourts.fuel_price.E10`").alias("E10"),
        F.col("`forecourts.fuel_price.B7P`").alias("B7P"),
        F.col("`forecourts.fuel_price.B7S`").alias("B7S"),
        F.col("`forecourts.fuel_price.B10`").alias("B10"),
        F.col("`forecourts.fuel_price.HV0`").alias("HV0")
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
  
