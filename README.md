# 🏙️ Chicago Crime Data Platform

End-to-end data platform ingesting Chicago crime reports since 2001.
Objectif Principal
Construire une plateforme de données de bout en bout (end-to-end), de niveau production, qui transforme des données brutes publiques (les crimes rapportés à Chicago depuis 2001) en informations analytiques fiables, accessibles et automatiquement mises à jour pour la prise de décision.

L'objectif n'est pas seulement de faire un dashboard, mais de démontrer une maîtrise complète du Data Engineering moderne : de l'ingestion à la visualisation, en passant par le traitement distribué, le stockage cloud, la qualité des données et l'orchestration.

Les 4 Objectifs Concrêts à Atteindre
1. Objectif Technique : Construire un pipeline automatisé et robuste
Créer un flux de données (pipeline) qui s'exécute tout seul chaque jour sans intervention humaine, allant de l'API publique de Chicago jusqu'à un dashboard interactif. Ce pipeline doit être :

Automatisé (Airflow déclenche tout seul)
Robuste (gestion des erreurs, retry, alertes)
Idempotent (re-lançable sans créer de doublons)
Scalable (capable de gérer 8 millions de lignes)
2. Objectif Métier : Rendre les données exploitables
Passer d'un fichier JSON illisible de 8 millions de lignes à des tables propres, modélisées en étoile (Star Schema), prêtes pour l'analyse. Le projet doit permettre de répondre à des questions métier concrètes :

L'évolution du crime depuis 2001 ?
Quels sont les types de crimes les plus fréquents ?
À quelles heures crimes sont-ils le plus rapportés ?
Quels districts sont les plus touchés ?
Le taux d'arrestation varie-t-il selon le type de crime ?
Week-end vs semaine : y a-t-il une différence ?
Y a-t-il des pics inhabituels ?
3. Objectif Qualité : Ne jamais faire confiance aux données aveuglément
Mettre en place des garde-fous (data quality checks) à chaque étape pour garantir que les données servies aux analystes sont justes :

Unicité des identifiants
Pas de valeurs nulles sur les colonnes critiques
Coordonnées géographiques valides (latitude/longitude dans la zone de Chicago)
Tests dbt sur les clés étrangères et plages de valeurs
## Architecture

Medallion Architecture (RAW → Bronze → Silver → Gold) on Google Cloud Platform.

## Tech Stack
- Python, PySpark, Airflow, dbt, BigQuery, Terraform, Docker, GitHub Actions

