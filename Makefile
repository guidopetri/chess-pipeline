build:
	poetry check && \
	docker build \
		--build-arg INSTALL_GROUPS="main" \
		-t chess-pipeline \
		.

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
