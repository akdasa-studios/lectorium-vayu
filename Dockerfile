FROM apache/airflow:2.10.2

# ---------------------------------------------------------------------------- #
#                                     Meta                                     #
# ---------------------------------------------------------------------------- #

LABEL org.opencontainers.image.description="Vayu"
LABEL org.opencontainers.image.source="https://github.com/akdasa-studios/lectorium"

# ---------------------------------------------------------------------------- #
#                             Install Dependencies                             #
# ---------------------------------------------------------------------------- #

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir deepgram-sdk anthropic jellyfish
