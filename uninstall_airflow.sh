# --volumes → supprime les volumes (donc la DB Airflow, logs, etc.)
# --rmi all → supprime aussi les images construites.
# --remove-orphans → nettoie d’éventuels conteneurs liés à d’anciens services qui ne sont plus dans le docker-compose.yaml
docker-compose down --volumes --rmi all --remove-orphans
