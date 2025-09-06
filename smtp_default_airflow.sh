docker-compose exec airflow-webserver airflow connections delete smtp_default
docker-compose exec airflow-webserver airflow connections add 'smtp_default' \
    --conn-type email \
    --conn-host mailhog \
    --conn-port 1025 \
    --conn-login '' \
    --conn-password '' \
    --conn-extra '{"from_email": "airflow@test.local", "enable_ssl": false, "enable_tls": false}'