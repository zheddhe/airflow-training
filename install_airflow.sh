#!/usr/bin/env bash

# Setup Airflow DataScientest env
# # cours
# wget https://dst-de.s3.eu-west-3.amazonaws.com/airflow_fr/docker-compose/docker-compose.yaml
# # exam
# wget https://dst-de.s3.eu-west-3.amazonaws.com/airflow_fr/eval/docker-compose.yaml
# Collect airflow terminal script (use with ./airflow.sh bash)
# wget https://dst-de.s3.eu-west-3.amazonaws.com/airflow_avance_fr/docker-compose/airflow.sh
mkdir -p ./dags ./logs ./plugins ./clean_data ./raw_files
sudo chmod -R 777 logs/ dags/ plugins/ clean_data/ raw_files/
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
chmod +x *.sh

wget https://dst-de.s3.eu-west-3.amazonaws.com/airflow_avance_fr/eval/data.csv -O clean_data/data.csv
echo '[]' >> raw_files/null_file.json