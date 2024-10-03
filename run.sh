#!/bin/bash
source .venv/bin/activate
PYTHONPATH=. AIRFLOW_HOME=$(pwd) airflow standalone
