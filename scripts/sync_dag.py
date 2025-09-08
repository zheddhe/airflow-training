import subprocess
import os

# Liste des DAGs connus par Airflow
db_dags = subprocess.check_output(["airflow", "dags", "list", "--output", "json"])

# Liste des DAGs présents en code (à parser avec dag_bag si tu veux être fiable)
code_dags = {f.replace(".py", "") for f in os.listdir("/opt/airflow/dags") if f.endswith(".py")}

# Détection des orphelins
orphans = set(db_dags) - code_dags

# Suppression
for dag_id in orphans:
    subprocess.run(["airflow", "dags", "delete", dag_id, "--yes"])