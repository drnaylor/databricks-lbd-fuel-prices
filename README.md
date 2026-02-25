# Opencast Learn by Doing -- Databricks Dashboard for Petrol Prices

This repository contains a Databricks workspace which assumes that within your tenant, you have:

* three catalogs, `bronze`, `silver` and `gold`
* within each catalog there is a schema called `petrol_prices`
* in `bronze.petrol_prices`, there is a volume called `csv`

In that volume, there should be three directories:

* `fueltypes`, which should contain one CSV file called `fuel_types.csv` that looks like this:

|fuel_type_code|fuel_type_description|
|--------------|---------------------|
|E5|Super Unleaded|
|E10|Unleaded|
|B7S|Diesel|
|B7P|Premium Diesel|
|B10|Biodiesel|
|HVO|Hydrogen|

* `postcode`, that could contain at least one CSV file with at least the following columns:
    * `postcode`
    * `latitude`
    * `longitude`
* `prices`, where fuel price CSVs from https://www.developer.fuel-finder.service.gov.uk/access-latest-fuelprices should be placed

Once that is done, set up an ETL Pipeline in Databricks to use the `petrol_prices_etl_pipeline` pipeline. Once that runs successfully, run the notebook in `metric_views`.

You can now import the dashboard in `dashboards` and perform the exploration that you wish (within the dashboard constraints!)
