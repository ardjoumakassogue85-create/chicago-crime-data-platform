"""Télécharge un échantillon de 1000 crimes pour l'EDA."""
import requests
import json
from pathlib import Path

URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
LIMIT = 1000

def main():
    print(f"Téléchargement de {LIMIT} crimes...")
    params = {"$limit": LIMIT, "$order": "date DESC"}
    
    response = requests.get(URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    
    output = Path("data/raw/sample_1000.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output, "w", encoding="utf-8") as f:
        json.dump(data, f, default=str, indent=2)
    
    print(f"✅ {len(data)} crimes téléchargés dans {output}")

if __name__ == "__main__":
    main()
