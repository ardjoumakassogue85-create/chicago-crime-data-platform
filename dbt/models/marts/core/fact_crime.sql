{# Partition BigQuery par MOIS (granularité adaptée au volume) #}
{# Clustering pour optimiser les filtres fréquents #}
{{ config(
    materialized='table',
    schema='marts',
    partition_by={
        "field": "crime_date",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=["primary_type", "district"]
) }}

WITH crimes AS (
    SELECT * FROM {{ ref('stg_crimes') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

dim_time AS (
    SELECT * FROM {{ ref('dim_time') }}
),

dim_crime AS (
    SELECT * FROM {{ ref('dim_crime') }}
),

dim_location AS (
    SELECT * FROM {{ ref('dim_location') }}
)

SELECT
    -- PK
    c.crime_id,
    
    -- FK vers dimensions
    d.date_key,
    -- time_key : HHMM calculé à la volée
    EXTRACT(HOUR FROM c.crime_datetime) * 100 
        + EXTRACT(MINUTE FROM c.crime_datetime) AS time_key,
    dc.crime_type_key,
    dl.location_key,
    
    -- Dimensions dégénérées (restent dans la fact)
    c.case_number,
    c.primary_type,
    c.district,
    
    -- Faits mesurés
    c.arrest,
    c.domestic,
    
    -- Pour partitionnement BigQuery
    c.crime_date,
    c.crime_datetime,
    
    -- Coordonnées (utiles pour les cartes)
    c.latitude,
    c.longitude

FROM crimes c
LEFT JOIN dim_date d 
    ON c.crime_date = d.full_date
LEFT JOIN dim_crime dc 
    ON COALESCE(c.iucr, 'UNKNOWN') = dc.iucr 
    AND COALESCE(c.primary_type, 'UNKNOWN') = dc.primary_type
    AND COALESCE(c.description, 'UNKNOWN') = dc.description
    AND COALESCE(c.fbi_code, 'UNKNOWN') = dc.fbi_code
LEFT JOIN dim_location dl 
    ON COALESCE(CAST(c.district AS STRING), 'UNKNOWN') = COALESCE(CAST(dl.district AS STRING), 'UNKNOWN')
    AND COALESCE(CAST(c.beat AS STRING), 'UNKNOWN') = COALESCE(CAST(dl.beat AS STRING), 'UNKNOWN')
    AND COALESCE(CAST(c.ward AS STRING), 'UNKNOWN') = COALESCE(CAST(dl.ward AS STRING), 'UNKNOWN')
    AND COALESCE(CAST(c.community_area AS STRING), 'UNKNOWN') = COALESCE(CAST(dl.community_area AS STRING), 'UNKNOWN')
    AND COALESCE(CAST(c.block AS STRING), 'UNKNOWN') = COALESCE(CAST(dl.block AS STRING), 'UNKNOWN')
    AND COALESCE(CAST(c.location_description AS STRING), 'UNKNOWN') = COALESCE(CAST(dl.location_description AS STRING), 'UNKNOWN')
