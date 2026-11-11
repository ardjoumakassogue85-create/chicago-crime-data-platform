.PHONY: setup test lint format run-ingestion run-spark run-dbt clean

setup:
    python3 -m venv .venv
    . .venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
    pip install -r requirements-dev.txt

test:
    . .venv/bin/activate
    pytest tests/ -v --cov=. --cov-report=term-missing

lint:
    . .venv/bin/activate
    ruff check .

format:
    . .venv/bin/activate
    black .
    ruff check . --fix

run-ingestion:
    . .venv/bin/activate
    python -m ingestion.main

run-spark:
    . .venv/bin/activate
    python -m spark.jobs.raw_to_bronze

run-dbt:
    cd dbt && dbt run

clean:
    find . -type d -name __pycache__ -exec rm -rf {} +
    find . -type f -name "*.pyc" -delete
