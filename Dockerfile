# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e

FROM python:3.14-trixie@sha256:b16c25b07f6c4e3822b9c381867b9f935f67671c13a53f7176049e4b96b6be5c AS builder

WORKDIR /build

ENV PIP_DISABLE_PIP_VERSION_CHECK=on
ENV PIP_NO_INPUT=on
ENV PIP_PREFER_BINARY=on
ENV PIP_PROGRESS_BAR=off

COPY . .

RUN pip install --no-cache-dir -r requirements.txt
RUN uv export --no-dev --no-editable | uv pip install --system --no-deps -r -

RUN curl -fsSL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xEE4D7792F748182B" \
        | gpg --dearmor -o /build/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
        https://packages.microsoft.com/debian/13/prod trixie main" \
        > /build/mssql-release.list

FROM python:3.14-slim-trixie@sha256:cd89c30eda26ee911a4611b18680086e0ea42e6f5f8cbe653d07fa7958c9cec8

LABEL org.opencontainers.image.authors="mex@rki.de"
LABEL org.opencontainers.image.description="ETL pipelines for the RKI Metadata Exchange."
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.url="https://github.com/robert-koch-institut/mex-extractors"
LABEL org.opencontainers.image.vendor="robert-koch-institut"

ENV PYTHONUNBUFFERED=1
ENV PYTHONOPTIMIZE=1

ENV DAGSTER_HOME=/app/dagster
ENV MEX_WORK_DIR=/app/work

WORKDIR /app

COPY --from=builder /build/microsoft-prod.gpg /usr/share/keyrings/microsoft-prod.gpg
COPY --from=builder /build/mssql-release.list /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install -y krb5-user msodbcsql18 unixodbc \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.14/site-packages /usr/local/lib/python3.14/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

RUN install -d -o 10001 -g 10001 /app /app/dagster /app/work

COPY --chown=10001:10001 assets /app/assets
COPY --chown=10001:10001 workspace.yaml /app/workspace.yaml
COPY --chown=10001:10001 dagster.yaml /app/dagster/dagster.yaml

USER 10001

EXPOSE 3000

ENTRYPOINT [ "dagster", "dev", "--host", "0.0.0.0", "-w", "workspace.yaml" ]
