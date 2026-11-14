# Dataset Profile

## Volume
- Total lignes : ~8.5 millions
- Période : 2001 → présent
- Update : quotidien (~1k-5k lignes/jour)

## Qualité observée (sur échantillon de 1000)
- Doublons `id` : ~0 (mais toujours vérifier en Silver)
- Null `latitude`/`longitude` : ~1-2%
- Null `location_description` : ~5%
- Erreurs de date : < 0.01%

## Distribution observée
- Type dominant : BATTERY (~30%), THEFT (~22%)
- Heure la plus active : 17h-19h (soir)
- Taux d'arrestation global : ~12%

## Anomalies connues
- Pics saisonniers : été > hiver
- Baisse pendant COVID-19 (2020)
- Colonnes `:@computed_region_*` présentes mais inutiles pour l'analyse

## Décisions de modélisation
- Grain de la fact table : 1 ligne = 1 crime
- Clé primaire : `id`
- Partitionnement : par `crime_year`
