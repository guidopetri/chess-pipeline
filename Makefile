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
	docker rm chess_pipeline_dev_coverage || true
	docker compose run \
	  --entrypoint=pytest \
	  --name chess_pipeline_dev_coverage \
	  chess_pipeline_dev \
	  -vv \
	  --cov=/app \
	  --cov-report term \
	  --cov-report json:/coverage.json \
	  --cov-report html:/cov_html
	docker cp chess_pipeline_dev_coverage:/coverage.json .
	docker cp chess_pipeline_dev_coverage:/cov_html .

coverage: build-dev ci-coverage

ci-build-serverless-function:
	docker run \
	  --rm \
	  -v $$(pwd):/app \
	  -e POETRY_HOME \
	  --entrypoint=/bin/bash \
	  chess-pipeline-dev \
	  -c "poetry show stockfish | grep version | awk -F' ' '{print \$$3}'" > _stockfish_lib_version

	STOCKFISH_LIB_VERSION=$$(cat _stockfish_lib_version); \
	docker run \
	  --rm \
	  -v $$(pwd):/home/app/function \
	  --workdir /home/app/function \
	  rg.fr-par.scw.cloud/scwfunctionsruntimes-public/python-dep:3.12 \
	  sh ./build_stockfish.sh $$STOCKFISH_LIB_VERSION
	echo "__version__ = '$$(git log -1 --format='format:%h')'" > cloud_function/_version.py
	sudo chown $$USER:$$USER -R package/
	zip -FSr cloud_function.zip cloud_function/ package/
	rm _stockfish_lib_version
	rm -rf package/

build-serverless-function: build-dev ci-build-serverless-function
