-- POSTCODES

CREATE MATERIALIZED VIEW gold.petrol_prices.postcodes AS
WITH latest_postcodes AS (
    SELECT MAX_BY(entry_id, ingestion_time) as entry_id
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
LEFT SEMI JOIN latest_postcodes l USING (entry_id);

-- CLEANED PRICES

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
    CASE WHEN motorway_service_station_flag THEN 'Motorway' WHEN supermarket_flag THEN 'Supermarket' ELSE 'Other' END AS forecourt_type,
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

--- UNCLEANED

CREATE MATERIALIZED VIEW gold.petrol_prices.uncleaned_latest_prices AS
WITH latest_entries AS (
    SELECT MAX(entry_timestamp) as entry_timestamp, forecourt_id
    FROM silver.petrol_prices.uncleaned_prices
    GROUP BY forecourt_id
)
SELECT
    name,
    forecourt_id,
    trading_name,
    brand_name,
    CASE WHEN motorway_service_station_flag THEN 'Motorway' WHEN supermarket_flag THEN 'Supermarket' ELSE 'Other' END AS forecourt_type,
    postcode,
    latitude,
    longitude,
    super_unleaded,
    unleaded,
    premium_diesel,
    diesel,
    biodiesel,
    hydrogen
FROM silver.petrol_prices.uncleaned_prices
LEFT SEMI JOIN latest_entries l USING (entry_timestamp, forecourt_id)
WHERE temporary_closure = FALSE AND permanent_closure = FALSE;