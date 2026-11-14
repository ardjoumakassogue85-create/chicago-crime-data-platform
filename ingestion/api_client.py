"""Client HTTP pour interroger l'API Socrata de Chicago."""
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from loguru import logger
from ingestion.config import config


class ChicagoAPIClient:
    """
    Wrapper autour de l'API Socrata.
    
    Pourquoi tenacity ?
    - Les APIs publiques peuvent avoir des pannes temporaires
    - On veut éviter qu'un timeout à 3h du matin casse tout le pipeline
    - tenacity implémente le pattern "retry with backoff" proprement
    """

    def __init__(self, base_url: str, app_token: str):
        """
        Args:
            base_url: URL de l'API (ex: https://data.cityofchicago.org/resource/ijjp-q8t2.json)
            app_token: Token Socrata (optionnel mais recommandé)
        """
        self.base_url = base_url
        
        # On utilise une Session requests pour réutiliser la connexion TCP
        # (plus rapide que de recréer une connexion à chaque requête)
        self.session = requests.Session()
        
        if app_token:
            # Le token va dans le header X-App-Token
            self.session.headers.update({"X-App-Token": app_token})

    @retry(
        # Stop après max_retries tentatives
        stop=stop_after_attempt(config.max_retries),
        # Attente exponentielle : 2s → 4s → 8s → 16s → 32s
        wait=wait_exponential(
            multiplier=config.retry_backoff,
            min=2,
            max=60
        ),
        # Retry seulement sur les erreurs réseau et 5xx
        retry=retry_if_exception_type(
            (requests.ConnectionError, requests.Timeout, requests.HTTPError)
        ),
        # Lever l'exception à la fin si tous les retries échouent
        reraise=True,
    )
    def fetch_page(
        self,
        offset: int,
        limit: int,
        where_clause: str | None = None
    ) -> list[dict]:
        """
        Récupère UNE page de données via l'API.
        
        Args:
            offset: Décalage (pour la pagination)
            limit: Nombre max de lignes à récupérer
            where_clause: Filtre SoQL (ex: "date between '2024-01-01' and '2024-01-02'")
            
        Returns:
            Liste de dictionnaires (chaque dict = 1 crime)
        """
        # Construction des paramètres de requête
        # Socrata utilise la convention $limit, $offset, $where, $order
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "date ASC",  # Tri par date pour avoir un ordre stable
        }
        if where_clause:
            params["$where"] = where_clause

        logger.info(f"Fetching offset={offset:,} limit={limit:,}")

        # Requête GET
        response = self.session.get(
            self.base_url,
            params=params,
            timeout=config.timeout
        )
        
        # Lève une exception si le code HTTP n'est pas 2xx
        # (déclenche le retry du décorateur @retry)
        response.raise_for_status()
        
        # Conversion du JSON en liste Python
        return response.json()

    def fetch_incremental(
        self,
        start_date: str,
        end_date: str
    ) -> list[dict]:
        """
        Récupère TOUS les enregistrements entre start_date et end_date.
        
        Args:
            start_date: Date de début (format 'YYYY-MM-DD')
            end_date: Date de fin (format 'YYYY-MM-DD')
            
        Returns:
            Liste complète des crimes sur la période
        """
        # Construction de la clause WHERE en SoQL (Socrata Query Language)
        where = f"date between '{start_date}' and '{end_date}'"
        
        all_records = []
        offset = 0

        # Boucle de pagination
        while True:
            page = self.fetch_page(
                offset=offset,
                limit=config.page_size,
                where_clause=where
            )
            
            # Si la page est vide → on a tout récupéré
            if not page:
                logger.info(f"Plus de données à offset={offset:,}")
                break

            # Ajoute les records à la liste globale
            all_records.extend(page)
            logger.info(f"Total accumulé : {len(all_records):,} records")

            # Si la page contient MOINS de lignes que limit → c'était la dernière
            if len(page) < config.page_size:
                break

            # Sinon, on passe à la page suivante
            offset += config.page_size

        logger.info(f"Total final : {len(all_records):,} records")
        return all_records