{{ config(
    materialized='view',
    tags=['staging']
) }}

-- Staging : 1-1 avec la source, juste renommage et cast
WITH source AS (
    SELECT * FROM {{ source('silver', 'crimes') }}
),

renamed AS (
    SELECT
        -- PK
        id                          AS crime_id,
        case_number                 AS case_number,
        
        -- Date/Time
        crime_datetime              AS crime_datetime,
        crime_date                  AS crime_date,
        year                        AS crime_year,
        crime_month                 AS crime_month,
        crime_day                   AS crime_day,
        crime_hour                  AS crime_hour,
        crime_day_of_week           AS day_of_week,
        crime_is_weekend            AS is_weekend,
        
        -- Crime info
        iucr                        AS iucr,
        primary_type                AS primary_type,
        description                 AS description,
        location_description        AS location_description,
        fbi_code                    AS fbi_code,
        
        -- Fait mesuré
        arrest                      AS arrest,
        domestic                    AS domestic,
        
        -- Localisation
        beat                        AS beat,
        district                    AS district,
        ward                        AS ward,
        community_area              AS community_area,
        block                       AS block,
        
        -- Géographie
        latitude                    AS latitude,
        longitude                   AS longitude
        
    FROM source
)

SELECT * FROM renamed