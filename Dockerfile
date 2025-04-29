# syntax=docker/dockerfile:1

# using bullseye because microsoft does not play nice with debian 12 signature verification yet
# https://learn.microsoft.com/en-us/answers/questions/1328834/debian-12-public-key-is-not-available
# debian 11 bullseye is on a LTS schedule until August 31st, 2026
FROM python:3.11 AS base

LABEL org.opencontainers.image.authors="mex@rki.de"
LABEL org.opencontainers.image.description="ETL pipelines for the RKI Metadata Exchange."
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.url="https://github.com/robert-koch-institut/mex-extractors"
LABEL org.opencontainers.image.vendor="robert-koch-institut"

ENV PYTHONUNBUFFERED=1
ENV PYTHONOPTIMIZE=1

ENV PIP_DISABLE_PIP_VERSION_CHECK=on
ENV PIP_NO_INPUT=on
ENV PIP_PREFER_BINARY=on
ENV PIP_PROGRESS_BAR=off

ENV DAGSTER_HOME=/app

WORKDIR /app

RUN adduser \
    --disabled-password \
    --gecos "" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "10001" \
    mex

COPY . .

RUN --mount=type=cache,target=/root/.cache/pip pip install -r locked-requirements.txt --no-deps

USER mex

ENTRYPOINT [ "all-extractors" ]
