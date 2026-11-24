{{ config(
    materialized='table',
    schema='marts'
) }}

-- Dimension Location : combinaisons uniques géographiques
WITH locations AS (
    SELECT
        district,
        beat,
        ward,
        community_area,
        block,
        location_description
    FROM {{ ref('stg_crimes') }}
    GROUP BY 1, 2, 3, 4, 5, 6
),

ranked AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY 
                COALESCE(CAST(district AS STRING), 'UNKNOWN'),
                COALESCE(CAST(beat AS STRING), 'UNKNOWN')
        ) AS location_key,
        *
    FROM locations
)

SELECT * FROM ranked
