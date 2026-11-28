# Chicago Crime — Dashboards (Looker Studio)

Un seul rapport Looker Studio, organisé en 4 pages.

**URL du rapport :** https://datastudio.google.com/s/mQLZw0LG2Cs

| Page | URL |
|---|---|
| 1. Vue d'ensemble | https://datastudio.google.com/s/mQLZw0LG2Cs |
| 2. Analyse temporelle | https://datastudio.google.com/s/mQLZw0LG2Cs |
| 3. Analyse géographique | https://datastudio.google.com/s/mQLZw0LG2Cs |
| 4. Métriques du pipeline | https://datastudio.google.com/s/mQLZw0LG2Cs |

> Ouvre le rapport, clique sur une page, puis copie l'URL affichée dans la barre du navigateur.

---

## Sources de données

| Source | Objet BigQuery | Utilisée par |
|---|---|---|
| Crimes | `chicago-crime-data-platform.silver_marts.vw_crimes_dashboard` | Pages 1 à 3 |
| Pipeline | `chicago-crime-data-platform.silver_marts.vw_pipeline_metrics` | Page 4 |

- **vw_crimes_dashboard** : vue construite sur `fact_crime` + `dim_time` + `dim_location`.
  Définition : [`vw_crimes_dashboard.sql`](vw_crimes_dashboard.sql).
  À jour après chaque `dbt run` quotidien.
- **vw_pipeline_metrics** : les 30 derniers runs de la table `silver.pipeline_runs`,
  enregistrés à la fin de chaque run du DAG (`airflow/plugins/pipeline_metrics.py`).
- **Identifiants** : identifiants du propriétaire (les lecteurs n'ont pas besoin
  d'accès à BigQuery ; les requêtes sont facturées sur le projet).
- **Fraîcheur des données** : cache Looker Studio de 12 heures par défaut.

### Champs de `vw_crimes_dashboard`

| Champ | Description |
|---|---|
| `crime_id`, `crime_date` | Identifiant du crime, date |
| `annee` | Année (texte, ex. `2024`) |
| `mois` | Mois, triable (`01-Jan` … `12-Déc`) |
| `jour_semaine` | Jour de la semaine, triable (`1-Lun` … `7-Dim`) |
| `heure` | Heure de 0 à 23 (agrégation réglée sur *Aucune* dans Looker Studio) |
| `type_jour` | `Semaine` / `Week-end` |
| `primary_type` | Type de crime |
| `district`, `community_area` | Localisation (texte) |
| `is_arrest`, `is_domestic` | 0 / 1 |
| `lat_long` | `latitude,longitude` (type *Géo › Latitude, longitude*) |

### Champs calculés (créés dans Looker Studio)

| Source | Champ | Formule | Type |
|---|---|---|---|
| Crimes | Total crimes | `COUNT(crime_id)` | Nombre |
| Crimes | Arrestations | `SUM(is_arrest)` | Nombre |
| Crimes | Crimes domestiques | `SUM(is_domestic)` | Nombre |
| Crimes | Taux d'arrestation | `SUM(is_arrest) / COUNT(crime_id)` | Pourcentage |
| Pipeline | Taux de succès | `SUM(IF(status = "success", 1, 0)) / COUNT(status)` | Pourcentage |

---

## Page 1 — Vue d'ensemble

Commande : sélecteur de période.

Indicateurs :
- Total crimes
- Arrestations
- Crimes domestiques
- Taux d'arrestation

Graphiques :
- Évolution des crimes dans le temps (série temporelle, granularité mensuelle)
- Top 10 des types de crimes (barres)
- Semaine / week-end (secteurs, `type_jour`)

## Page 2 — Analyse temporelle

Graphiques :
- Crimes par heure de la journée (barres, `heure` de 0 à 23)
- Mois × jour de la semaine (tableau croisé avec carte de densité, `mois` × `jour_semaine`)
- Tendance mensuelle, une courbe par année (courbes, `mois` réparti par `annee`)

## Page 3 — Analyse géographique

Commande : sélecteur de période, **30 derniers jours** par défaut (la carte reste lisible et rapide).

Graphiques :
- Carte des crimes (Google Maps, calque carte de densité sur `lat_long`)
- Top 10 des districts (barres)
- Statistiques par community area (tableau : nombre de crimes, taux d'arrestation)

⚠️ Les coordonnées géographiques sont approximatives, conformément à la politique de Chicago Open Data.

## Page 4 — Métriques du pipeline

Indicateurs :
- Taux de succès (30 derniers runs)

Graphiques :
- Lignes traitées par date de données (série temporelle, `data_date` × `rows_processed`)
- Durée moyenne de traitement (courbes, `processing_duration_seconds`)
- Runs en échec (tableau : `data_date`, `status`, `failed_tasks`, filtre `status = failed`)

Non disponibles (pas encore calculés par le pipeline) : coût BigQuery, score de qualité des données.

---

## Limites connues

- **Retard d'environ 10 jours** : l'API de Chicago publie les données avec environ
  8 jours de retard, donc le DAG traite le jour J-10. Les dashboards ont environ
  10 jours de retard sur aujourd'hui.
- **Signalements tardifs** : les crimes signalés après le traitement de leur journée
  ne sont pas récupérés (l'extraction se base sur la date du crime, pas sur `updated_on`).
- **Coordonnées approximatives** : la ville décale les positions au niveau du pâté de maisons.
- **Taux de succès** : calculé sur les 30 derniers runs, et non sur 30 jours calendaires.
- **Performances** : les pages 1 à 3 interrogent une vue, donc chaque graphique relit
  `fact_crime`. Si le rapport devient lent, transformer la vue en table rafraîchie par dbt.