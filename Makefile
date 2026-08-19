.PHONY: up down logs ps test clean init-db seed-db

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

test:
	pytest

init-db:
	python -m src.utils.database

seed-db:
	python -m src.synthetic_data.generate_retail_data

clean:
	python -c "import shutil; shutil.rmtree('.pytest_cache', ignore_errors=True)"
	python -c "import shutil; from pathlib import Path; [shutil.rmtree(path, ignore_errors=True) for path in Path('.').rglob('__pycache__') if not {'.git', '.venv', 'venv', 'data'}.intersection(path.parts)]"
