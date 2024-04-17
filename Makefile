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

integration-test: setup-postgres build
	docker compose run --rm chess_pipeline \
	  --module chess_pipeline \
	  CopyGames \
	  --player Zhigalko_Sergei \
	  --perf-type bullet \
	  --local-stockfish

shell:
	docker run --rm -it --entrypoint=/bin/bash chess-pipeline-dev

build-dev:
	docker build \
		--build-arg INSTALL_GROUPS="main,dev" \
		-t chess-pipeline-dev \
		.

pyright: build-dev
	docker run \
		--rm -it \
		--entrypoint=pyright \
		chess-pipeline-dev \
		--project /app/pyproject.toml \
		/app

pytest: build-dev
	docker run \
		--rm -it \
		--entrypoint=pytest \
		chess-pipeline-dev \
		-vv

coverage: build-dev
	docker run \
		--rm -it \
		--entrypoint=pytest \
		chess-pipeline-dev \
		-vv \
		--cov=src/ \
		--cov-report term \
		--cov-report json:coverage.json \
		--cov-report html:cov_html
