-- -- DATE DIMENSION

CREATE MATERIALIZED VIEW gold.petrol_prices.dates AS
WITH min_date AS (
    SELECT CAST(MIN(price_timestamp) as DATE) AS `date` FROM silver.petrol_prices.prices
),
dates AS (
    SELECT EXPLODE(SEQUENCE(`date`, CURRENT_DATE(), INTERVAL 1 DAY)) AS `date`
    FROM min_date
)
SELECT
    INT(DATE_FORMAT(`date`, 'yyyyMMdd')) AS date_key,
    `date`
FROM dates;

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
WHERE `__END_AT` IS NULL AND NOT temporary_closure AND NOT permanent_closure;

-- -- FORECOURT HISTORY

CREATE MATERIALIZED VIEW gold.petrol_prices.forecourt_history AS
WITH initial AS (
    SELECT DISTINCT
        entry_timestamp,
        INT(DATE_FORMAT(entry_timestamp, 'yyyyMMdd')) AS date_key,
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
    -- We don't care what this ID is, just that it's something we can link to. Easiest way to do this is just to hash datekey and forecourt_id
    -- We need this specific ID for a join to the price history table later
    xxhash64(date_key, forecourt_id) AS entry_id,
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
