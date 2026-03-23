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


