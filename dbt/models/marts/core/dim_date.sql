{{ config(
    materialized='table',
    schema='marts'
) }}

-- Dimension Date : générée à partir des dates distinctes présentes dans les crimes
WITH date_spine AS (
    SELECT DISTINCT crime_date
    FROM {{ ref('stg_crimes') }}
    WHERE crime_date IS NOT NULL
),

dim_date AS (
    SELECT
        -- Clé : YYYYMMDD en INT64
        CAST(FORMAT_DATE('%Y%m%d', crime_date) AS INT64) AS date_key,
        crime_date AS full_date,
        EXTRACT(YEAR FROM crime_date) AS year,
        EXTRACT(MONTH FROM crime_date) AS month,
        EXTRACT(QUARTER FROM crime_date) AS quarter,
        EXTRACT(DAY FROM crime_date) AS day_of_month,
        EXTRACT(DAYOFYEAR FROM crime_date) AS day_of_year,
        EXTRACT(DAYOFWEEK FROM crime_date) AS day_of_week,
        FORMAT_DATE('%A', crime_date) AS day_name,
        FORMAT_DATE('%B', crime_date) AS month_name,
        EXTRACT(ISOWEEK FROM crime_date) AS iso_week,
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM crime_date) IN (1, 7) 
            THEN TRUE ELSE FALSE 
        END AS is_weekend
    FROM date_spine
)

SELECT * FROM dim_date
