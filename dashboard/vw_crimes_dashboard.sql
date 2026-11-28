-- Vue utilisée par les pages 1 à 3 du rapport Looker Studio.
-- fact_crime + heure (dim_time) + community_area (dim_location),
-- avec des champs prêts pour Looker Studio (textes triables, 0/1, lat_long).
--
-- Créer / mettre à jour :
--   bq query --use_legacy_sql=false < dashboard/vw_crimes_dashboard.sql

CREATE OR REPLACE VIEW `chicago-crime-data-platform.silver_marts.vw_crimes_dashboard` AS
SELECT
  f.crime_id,
  f.crime_date,
  CAST(EXTRACT(YEAR FROM f.crime_date) AS STRING) AS annee,
  CONCAT(FORMAT_DATE("%m", f.crime_date), "-",
    ["Jan","Fév","Mar","Avr","Mai","Juin","Juil","Août","Sep","Oct","Nov","Déc"][OFFSET(EXTRACT(MONTH FROM f.crime_date) - 1)]) AS mois,
  CONCAT(FORMAT_DATE("%u", f.crime_date), "-",
    ["Lun","Mar","Mer","Jeu","Ven","Sam","Dim"][OFFSET(CAST(FORMAT_DATE("%u", f.crime_date) AS INT64) - 1)]) AS jour_semaine,
  COALESCE(t.hour, EXTRACT(HOUR FROM f.crime_datetime)) AS heure,
  IF(EXTRACT(DAYOFWEEK FROM f.crime_date) IN (1, 7), "Week-end", "Semaine") AS type_jour,
  f.primary_type,
  CAST(f.district AS STRING) AS district,
  CAST(l.community_area AS STRING) AS community_area,
  IF(f.arrest, 1, 0) AS is_arrest,
  IF(f.domestic, 1, 0) AS is_domestic,
  IF(f.latitude IS NULL OR f.longitude IS NULL, NULL,
     CONCAT(CAST(f.latitude AS STRING), ",", CAST(f.longitude AS STRING))) AS lat_long
FROM `chicago-crime-data-platform.silver_marts.fact_crime` f
LEFT JOIN `chicago-crime-data-platform.silver_marts.dim_time` t ON f.time_key = t.time_key
LEFT JOIN `chicago-crime-data-platform.silver_marts.dim_location` l ON f.location_key = l.location_key;