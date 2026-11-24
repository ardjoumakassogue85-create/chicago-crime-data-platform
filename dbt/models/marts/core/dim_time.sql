{{ config(
    materialized='table',
    schema='marts'
) }}

-- Dimension Temps : générée à partir des heures/min distinctes
WITH time_spine AS (
    SELECT DISTINCT 
        crime_hour,
        EXTRACT(MINUTE FROM crime_datetime) AS minute
    FROM {{ ref('stg_crimes') }}
),

dim_time AS (
    SELECT
        -- Clé : HHMM (ex: 1430)
        crime_hour * 100 + minute AS time_key,
        crime_hour AS hour,
        minute AS minute,
        CASE
            WHEN crime_hour BETWEEN 5 AND 11 THEN 'morning'
            WHEN crime_hour BETWEEN 12 AND 17 THEN 'afternoon'
            WHEN crime_hour BETWEEN 18 AND 21 THEN 'evening'
            ELSE 'night'
        END AS period
    FROM time_spine
)

SELECT * FROM dim_time
