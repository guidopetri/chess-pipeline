FROM python:3.12-slim-bookworm
LABEL maintainer="github.com/guidopetri"

# keeps python from generating .pyc files in the container
ARG PYTHONDONTWRITEBYTECODE 1
# turns off buffering for easier container logging
ARG PYTHONUNBUFFERED 1
ARG PIP_DISABLE_PIP_VERSION_CHECK=on

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    python3-dev \
    libpq-dev \
    cron \
    make \
    g++ \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives

RUN mkdir /app
WORKDIR /app

ARG POETRY_VERSION=1.7.0
ARG POETRY_HOME=/opt/poetry
ARG POETRY_NO_INTERACTION=1
ARG POETRY_VIRTUALENVS_CREATE=false
RUN curl -sSL https://install.python-poetry.org | python3 -

ARG TARGETARCH
RUN if [ "$TARGETARCH" = "amd64" ] ; \
    then echo x86-64 ; \
    elif [ "$TARGETARCH" = "arm64" ] ; \
    then echo armv8 ; \
    fi >> /stockfish_arch
RUN curl -L --output stockfish_13.tar.gz https://github.com/official-stockfish/Stockfish/archive/refs/tags/sf_13.tar.gz && \
  mkdir -p /stockfish_src && \
  tar -xzvf stockfish_13.tar.gz -C /stockfish_src && \
  make -C /stockfish_src/Stockfish-sf_13/src -j profile-build ARCH=$(cat /stockfish_arch) && \
  cp /stockfish_src/Stockfish-sf_13/src/stockfish / && \
  rm -r /stockfish_src

COPY pyproject.toml poetry.lock /app

RUN $POETRY_HOME/bin/poetry install --only main --no-root --no-ansi --no-cache \
    && $POETRY_HOME/bin/poetry cache clear pypi --all

ARG INSTALL_GROUPS="main"
RUN $POETRY_HOME/bin/poetry install --with $INSTALL_GROUPS --no-root --no-ansi --no-cache \
    && $POETRY_HOME/bin/poetry cache clear pypi --all

COPY src/ .
COPY tests/ .

ENV PYTHONPATH /app

ENTRYPOINT ["cron", "-f"]
