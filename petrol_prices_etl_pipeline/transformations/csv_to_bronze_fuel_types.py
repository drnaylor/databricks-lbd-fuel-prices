# Import modules
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField

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
  