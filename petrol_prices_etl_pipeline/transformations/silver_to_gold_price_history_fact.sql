-- -- CLEANED PRICES HISTORY (by date)

-- We take the price of fuel at the latest point of the day
CREATE MATERIALIZED VIEW gold.petrol_prices.price_history AS
WITH initial AS (
    SELECT
        price_timestamp AS last_update,
        INT(DATE_FORMAT(price_timestamp, 'yyyyMMdd')) AS date_key,
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
        -- We use a far in future date key rather than today to avoid missing today off when
        -- we remove a day from this later.
        LEAD(date_key, 1, 99991231) OVER (PARTITION BY forecourt_id, fuel_type_code ORDER BY date_key) AS end_date_key,
        postcode,
        forecourt_id,
        fuel_type_code,
        price,
        ROW_NUMBER() OVER (PARTITION BY date_key, forecourt_id, fuel_type_code ORDER BY last_update DESC) AS rn -- ensures the last entry is given a row number of 1
    FROM initial
),
forecourt_ranges AS (
    SELECT 
        date_key AS start_date_key,
        LEAD(date_key) OVER (PARTITION BY forecourt_id ORDER BY date_key) AS end_date_key,
        forecourt_id,
        entry_id,
        (temporary_closure OR permanent_closure) AS closed
    FROM gold.petrol_prices.forecourt_history
),
forecourt_ranges2 AS (
    SELECT
        start_date_key,
        -- COALESCE to avoid null semantics
        COALESCE(end_date_key, 99991231) as end_date_key,
        forecourt_id,
        entry_id,
        closed
    FROM forecourt_ranges
    WHERE start_date_key <> end_date_key -- we just want end of day, so we don't want multiple records on the same day,
                                         -- this will remove same day changes
),
joined_forecourt_price_data AS (
    SELECT
        o.last_update,
        o.date_key,
        o.end_date_key,
        o.postcode,
        o.forecourt_id,
        o.fuel_type_code,
        o.price,
        fr.entry_id AS forecourt_entry_id
    FROM ordered o
    INNER JOIN forecourt_ranges2 fr ON o.date_key BETWEEN fr.start_date_key AND fr.end_date_key 
        AND fr.forecourt_id = o.forecourt_id
    WHERE o.rn = 1 AND o.price IS NOT NULL AND NOT fr.closed
)
-- Finally, we explode the date column to be able to join on a date by date basis
-- our grain is per day, and we know from previous transformations that the date key and
-- end date key are not the same
-- 
-- We do it via a join because our date keys are integers, so when going over a month or year
-- boundary, we will get a lot of entries that don't represent dates (e.g. 20260332 is between
-- 31st March and 1st April), so we join to a known good date table that runs until today to
-- ensure that we only get valid dates.
SELECT
    jfd.last_update,
    gp.date_key AS date_key,
    jfd.postcode,
    jfd.forecourt_id,
    jfd.forecourt_entry_id,
    jfd.fuel_type_code,
    jfd.price
FROM joined_forecourt_price_data jfd
INNER JOIN gold.petrol_prices.dates gp 
    -- -1 as BETWEEN is inclusive and we want a half open range
    -- except in the case of "today", but given that is represented by the date key
    -- 99991231, this will still get included.
    ON gp.date_key BETWEEN jfd.date_key AND jfd.end_date_key - 1; 
