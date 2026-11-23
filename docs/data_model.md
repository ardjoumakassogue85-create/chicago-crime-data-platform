# Chicago Crime - Data Model

## Architecture : Star Schema

                    dim_date
                       │
                       │
dim_crime ─────── fact_crime ─────── dim_location │ │ dim_time


## Grain de la fact table
- **1 ligne = 1 crime rapporté**

## Tables

### fact_crime
| Colonne | Type | Description |
|---------|------|-------------|
| crime_id | BIGINT | PK unique |
| date_key | INT64 | FK → dim_date |
| time_key | INT64 | FK → dim_time |
| crime_type_key | INT64 | FK → dim_crime |
| location_key | INT64 | FK → dim_location |
| case_number | STRING | Dimension dégénérée |
| arrest | BOOLEAN | Fait mesuré |
| domestic | BOOLEAN | Fait mesuré |
| crime_date | DATE | Pour partition BQ |

### dim_date
- Grain : 1 jour
- Cardinalité : ~8 000 lignes (2001 → aujourd'hui)
- Clé : YYYYMMDD (ex: 20240101)

### dim_time
- Grain : 1 minute
- Cardinalité : 1440 lignes
- Clé : HHMM (ex: 1430)

### dim_crime
- Grain : 1 combinaison unique (iucr, primary_type, description)
- Cardinalité : ~400 combinaisons

### dim_location
- Grain : 1 combinaison unique (district, beat, ward, community_area, block, location_description)
- Cardinalité : ~30 000 combinaisons

## Règles de modélisation
1. **Surrogate keys** : Toutes les FK sont des INT64 séquentiels (pas les codes métier)
2. **Slowly Changing Dimensions** : Type 1 (overwrite) pour dim_crime et dim_location
3. **Conformed dimension** : dim_date partagée
4. **Degenerate dimension** : case_number reste dans la fact

## Partitionnement BigQuery
- fact_crime : partition par MONTH sur crime_date
- Clustering : primary_type, district
