# airflow-training
Practice airflow scheduling in context of datascientest training

```bash
# init environnement and init smtp connection for mails
./install_airflow.sh
./smtp_default_airflow.sh

# create airflow containers
./up_airflow.sh

# pause/restart airflow containers
./stop_airflow.sh
./start_airflow.sh

# recreate airflow container -> soft reset
./reset_airflow.sh

# remove airflow container and network -> reset
./down_airflow.sh

# remove airflow container / network / volumes (Postgresql volume included) -> hard reset
./clean_airflow.sh
```
