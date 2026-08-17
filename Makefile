.PHONY: up down logs ps test clean

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

clean:
	python -c "import shutil; shutil.rmtree('.pytest_cache', ignore_errors=True)"
	python -c "import shutil; from pathlib import Path; [shutil.rmtree(path, ignore_errors=True) for path in Path('.').rglob('__pycache__') if not {'.git', '.venv', 'venv', 'data'}.intersection(path.parts)]"
