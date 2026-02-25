-- -- POSTCODES

CREATE MATERIALIZED VIEW gold.petrol_prices.postcodes AS
WITH latest_postcodes AS (
    SELECT MAX_BY(postcode, ingestion_time) as postcode
    FROM silver.petrol_prices.postcodes
    GROUP BY postcode
)
SELECT DISTINCT
    postcode,
    regexp_substr(UPPER(postcode), '^[A-Z]{1,2}') AS geographic_postcode,
    regexp_substr(UPPER(postcode), '^[A-Z]{1,2}[0-9][0-9A-Z]?') AS outward_postcode,
    latitude,
    longitude
FROM silver.petrol_prices.postcodes
LEFT SEMI JOIN latest_postcodes l USING (postcode);

-- -- FUEL TYPES

CREATE MATERIALIZED VIEW gold.petrol_prices.fuel_types AS
SELECT DISTINCT * FROM silver.petrol_prices.fuel_types;

-- -- FORECOURTS

CREATE MATERIALIZED VIEW gold.petrol_prices.active_forecourts AS
SELECT DISTINCT
    entry_timestamp,
    forecourt_id,
    trading_name,
    brand_name,
    temporary_closure,
    permanent_closure,
    CASE WHEN motorway_service_station_flag THEN 'Motorway' WHEN supermarket_flag THEN 'Supermarket' ELSE 'Other' END AS forecourt_type,
    postcode,
    reported_latitude,
    reported_longitude
FROM silver.petrol_prices.forecourts
WHERE __END_AT IS NULL AND temporary_closure = FALSE AND permanent_closure = FALSE;

-- -- ORIGINAL PRICES

CREATE MATERIALIZED VIEW gold.petrol_prices.latest_original_prices AS
WITH latest_entries AS (
    SELECT MAX(price_timestamp) as price_timestamp, forecourt_id, fuel_type_code
    FROM silver.petrol_prices.prices
    GROUP BY forecourt_id, fuel_type_code
)
SELECT
    price_timestamp AS last_update,
    postcode,
    forecourt_id,
    fuel_type_code,
    original_price
FROM silver.petrol_prices.prices
LEFT SEMI JOIN latest_entries l USING (price_timestamp, forecourt_id, fuel_type_code);

-- -- CLEANED PRICES

CREATE MATERIALIZED VIEW gold.petrol_prices.latest_prices AS
WITH latest_entries AS (
    SELECT MAX(price_timestamp) as price_timestamp, forecourt_id, fuel_type_code
    FROM silver.petrol_prices.prices
    GROUP BY forecourt_id, fuel_type_code
)
SELECT
    price_timestamp AS last_update,
    postcode,
    forecourt_id,
    fuel_type_code,
    price
FROM silver.petrol_prices.prices
LEFT SEMI JOIN latest_entries l USING (price_timestamp, forecourt_id, fuel_type_code);
