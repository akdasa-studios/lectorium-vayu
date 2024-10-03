# ---------------------------------------------------------------------------- #
#                                  Base Image                                  #
# ---------------------------------------------------------------------------- #

FROM python:3.12-slim as base

WORKDIR /akd-studios/lectorium/modules/services/vayu
RUN echo "Install dependencies"
    && sudo apt-get install -y --no-install-recommends \
    && g++ protobuf-compiler libprotobuf-dev libpq-dev

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# ---------------------------------------------------------------------------- #
#                                 Release Image                                #
# ---------------------------------------------------------------------------- #

FROM base as release

COPY . .
CMD ["python", "main.py"]
