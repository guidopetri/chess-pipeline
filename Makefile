build:
	poetry check && \
	docker build \
	  --build-arg INSTALL_GROUPS="main" \
	  -t chess-pipeline \
	  .

setup-postgres:
	docker stop postgres || true
	docker rm postgres || true
	docker volume rm chess-pipeline_postgres_data || true
	docker compose up -d postgres

ci-e2e-test: setup-postgres
	docker compose run --rm chess_pipeline \
	  --module chess_pipeline \
	  CopyGames \
	  --player thibault \
	  --perf-type bullet \
	  --since 2024-01-29 \
	  --single-day \
	  --local-stockfish

e2e-test: build ci-e2e-test

shell: build-dev
	docker compose run \
	  --rm -it \
	  --entrypoint=/bin/bash \
	  chess_pipeline_dev

build-dev:
	docker build \
	  --build-arg INSTALL_GROUPS="main,dev" \
	  -t chess-pipeline-dev \
	  .

ci-pyright:
	docker compose run \
	  --rm \
	  --entrypoint=pyright \
	  chess_pipeline_dev \
	  --project /app/pyproject.toml \
	  /app

pyright: build-dev ci-pyright

ci-pytest:
	docker compose run \
	  --rm \
	  --entrypoint=pytest \
	  chess_pipeline_dev \
	  -vv

pytest: build-dev ci-pytest

ci-coverage:
	docker compose run \
	  --rm -it \
	  --entrypoint=pytest \
	  chess_pipeline_dev \
	  -vv \
	  --cov=src/ \
	  --cov-report term \
	  --cov-report json:coverage.json \
	  --cov-report html:cov_html


coverage: build-dev ci-coverage
