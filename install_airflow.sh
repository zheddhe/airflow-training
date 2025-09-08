#!/usr/bin/env bash

# Setup Airflow DataScientest env
# # cours
# wget https://dst-de.s3.eu-west-3.amazonaws.com/airflow_fr/docker-compose/docker-compose.yaml
# # exam
# wget https://dst-de.s3.eu-west-3.amazonaws.com/airflow_fr/eval/docker-compose.yaml
mkdir -p ./dags ./logs ./plugins ./scripts
sudo chmod -R 777 logs/
sudo chmod -R 777 dags/
sudo chmod -R 777 plugins/
sudo chmod -R 777 scripts/
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
chmod +x start_airflow.sh

# Collect airflow terminal script (use with ./airflow.sh bash)
# wget https://dst-de.s3.eu-west-3.amazonaws.com/airflow_avance_fr/docker-compose/airflow.sh
chmod +x airflow.sh