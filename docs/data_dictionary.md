# Chicago Crimes — Data Dictionary

**Source** : https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2  
**API endpoint** : `https://data.cityofchicago.org/resource/ijzp-q8t2.json`  
**Volume** : ~8 millions de lignes depuis 2001  
**Update** : quotidienne

## Colonnes métier

| Colonne | Type (API) | Type Spark | Description | Nullable |
|---------|------------|------------|-------------|----------|
| `id` | integer | IntegerType | Identifiant unique du crime | NO |
| `case_number` | string | StringType | Numéro de dossier police (peut avoir doublons) | YES |
| `date` | timestamp | TimestampType | Date et heure de l'incident | NO |
| `block` | string | StringType | Adresse tronquée (100 block, ex: "0XX N MAIN ST") | YES |
| `iucr` | string | StringType | Illinois Uniform Crime Reporting code | YES |
| `primary_type` | string | StringType | Classification principale (THEFT, BATTERY, ...) | NO |
| `description` | string | StringType | Description secondaire | NO |
| `location_description` | string | StringType | Type de lieu (STREET, APARTMENT, ...) | YES |
| `arrest` | boolean | BooleanType | Arrestation effectuée | NO |
| `domestic` | boolean | BooleanType | Incident domestique | NO |
| `beat` | integer | IntegerType | Secteur de patrouille | YES |
| `district` | integer | IntegerType | District de police | YES |
| `ward` | integer | IntegerType | Arrondissement | YES |
| `community_area` | integer | IntegerType | Zone communautaire | YES |
| `fbi_code` | string | StringType | Code classification FBI | YES |
| `x_coordinate` | integer | IntegerType | Coordonnée X (State Plane) - obsolète | YES |
| `y_coordinate` | integer | IntegerType | Coordonnée Y (State Plane) - obsolète | YES |
| `year` | integer | IntegerType | Année de l'incident | NO |
| `updated_on` | timestamp | TimestampType | Date de mise à jour | YES |
| `latitude` | double | DoubleType | Latitude (approximative) | YES |
| `longitude` | double | DoubleType | Longitude (approximative) | YES |
| `location` | string | StringType | "{latitude, longitude}" combiné | YES |

## Colonnes techniques (Socrata)

| Colonne | Type | Description |
|---------|------|-------------|
| `:id` | string | UID interne Socrata |
| `:version` | string | Version de l'enregistrement |
| `:created_at` | timestamp | Date de création dans Socrata |
| `:updated_at` | timestamp | Date de maj dans Socrata |
| `:@computed_region_*` | integer | Codes géographiques (wards, zones) pour cartes Socrata |

## Notes importantes

1. **`date`** est renvoyée au format ISO 8601 : `"2024-01-15T12:30:00.000"`
2. Les **coordonnées** sont décalées aléatoirement dans le même bloc pour protéger la vie privée
3. **`case_number`** n'est PAS unique (un même dossier peut avoir plusieurs enregistrements)
4. **`id`** EST unique (clé primaire)
5. `Ward` et `Community Area` peuvent être vides (chaîne vide, pas null)
6. `x_coordinate` / `y_coordinate` sont obsolètes → utiliser `latitude`/`longitude`
7. Les colonnes `:@computed_region_*` sont utiles uniquement pour la cartographie Socrata, on les supprimera en Silver

## Avertissements officiels

> **La précision géographique est approximative.** Les adresses sont tronquées au niveau du bloc (100 block). Les coordonnées latitude/longitude sont déplacées aléatoirement pour protéger la vie privée des victimes. Ce dataset **ne doit pas** être utilisé pour accuser des quartiers spécifiques.
