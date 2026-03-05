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
FROM silver.petrol_prices.forecourts;

-- -- FORECOURT HISTORY

CREATE MATERIALIZED VIEW gold.petrol_prices.forecourt_history AS
WITH initial AS (
    SELECT DISTINCT
        entry_timestamp,
        DATE_FORMAT(entry_timestamp, 'yyyyMMdd') AS date_key,
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
),
ordered AS (
    SELECT 
        date_key,
        forecourt_id,
        trading_name,
        brand_name,
        temporary_closure,
        permanent_closure,
        forecourt_type,
        postcode,
        reported_latitude,
        reported_longitude,
        ROW_NUMBER() OVER (PARTITION BY date_key, forecourt_id ORDER BY entry_timestamp DESC) AS rn -- ensures the last entry is given a row number of 1
    FROM initial
)
SELECT
    date_key,
    forecourt_id,
    trading_name,
    brand_name,
    temporary_closure,
    permanent_closure,
    forecourt_type,
    postcode,
    reported_latitude,
    reported_longitude
FROM ordered
WHERE rn = 1;

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

-- -- CLEANED LATEST PRICES

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

-- -- CLEANED PRICES HISTORY (by date)

-- We take the price of fuel at the latest point of the day
CREATE MATERIALIZED VIEW gold.petrol_prices.price_history AS
WITH initial AS (
    SELECT
        price_timestamp AS last_update,
        DATE_FORMAT(price_timestamp, 'yyyyMMdd') AS date_key,
        postcode,
        forecourt_id,
        fuel_type_code,
        price
    FROM silver.petrol_prices.prices
),
ordered AS (
    SELECT
        last_update,
        date_key,
        postcode,
        forecourt_id,
        fuel_type_code,
        price,
        ROW_NUMBER() OVER (PARTITION BY date_key, forecourt_id, fuel_type_code ORDER BY last_update DESC) AS rn -- ensures the last entry is given a row number of 1
    FROM initial
)
SELECT
    last_update,
    date_key,
    postcode,
    forecourt_id,
    fuel_type_code,
    price
FROM ordered
WHERE rn = 1 AND price IS NOT NULL;

-- DATE DIMENSION

CREATE MATERIALIZED VIEW gold.petrol_prices.dates AS
WITH min_date AS (
    SELECT DATE_TRUNC(MIN(price_timestamp), 'DAY') AS first_date FROM silver.petrol_prices.prices
),
dates AS (
    SELECT EXPLODE(SEQUENCE(first_date, CURRENT_DATE(), INTERVAL 1 DAY)) AS date
    FROM min_date
)
SELECT
    DATE_FORMAT(date, 'yyyyMMdd') AS date_key,
    date
FROM dates;
