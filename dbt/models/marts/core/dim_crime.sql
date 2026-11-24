{{ config(
    materialized='table',
    schema='marts'
) }}

-- Dimension Crime : combinaisons uniques de (iucr, primary_type, description)
WITH crimes AS (
    SELECT 
        COALESCE(iucr, 'UNKNOWN') AS iucr,
        COALESCE(primary_type, 'UNKNOWN') AS primary_type,
        COALESCE(description, 'UNKNOWN') AS description,
        COALESCE(fbi_code, 'UNKNOWN') AS fbi_code
    FROM {{ ref('stg_crimes') }}
    GROUP BY 1, 2, 3, 4
),

ranked AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY iucr, primary_type) AS crime_type_key,
        *
    FROM crimes
)

SELECT * FROM ranked
