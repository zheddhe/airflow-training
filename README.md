# airflow-training
Practice airflow scheduling in context of datascientest training

```bash
# init environnement and init smtp connection for mails
./install_airflow.sh
./smtp_default_airflow.sh

# first start of the docker compose environment
./up_airflow.sh

# pause/restart with the following
./stop_airflow.sh
./start_airflow.sh

# stop the docker compose environment
./down_airflow.sh

# fully clean the docker compose environment
./clean_airflow.sh
```
