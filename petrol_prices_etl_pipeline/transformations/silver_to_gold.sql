CREATE MATERIALIZED VIEW gold.petrol_prices.latest_prices AS
WITH latest_entries AS (
    SELECT MAX(entry_timestamp) as entry_timestamp, forecourt_id
    FROM silver.petrol_prices.prices
    GROUP BY forecourt_id
)
SELECT
    name,
    forecourt_id,
    trading_name,
    brand_name,
    motorway_service_station_flag AS is_motorway_service_station,
    supermarket_flag AS is_supermarket,
    postcode,
    latitude,
    longitude,
    super_unleaded,
    unleaded,
    premium_diesel,
    diesel,
    biodiesel,
    hydrogen
FROM silver.petrol_prices.prices
LEFT SEMI JOIN latest_entries l USING (entry_timestamp, forecourt_id)
WHERE temporary_closure = FALSE AND permanent_closure = FALSE;