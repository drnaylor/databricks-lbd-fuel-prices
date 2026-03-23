# Import modules
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DecimalType

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