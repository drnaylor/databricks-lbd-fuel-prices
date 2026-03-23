# Import modules
from pyspark import pipelines as dp

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
