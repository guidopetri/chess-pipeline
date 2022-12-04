FROM python:3.9-slim-bullseye

RUN apt-get update -qq && apt-get install -y \
  gcc \
  libpq-dev

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ /app
WORKDIR /app
ENV PYTHONPATH=/app

ENTRYPOINT ["/bin/bash"]
