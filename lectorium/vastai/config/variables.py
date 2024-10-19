from airflow.models import Variable

Variable.setdefault("vastai_api_key", "")
Variable.setdefault("vastai_private_ssh_key", "")
