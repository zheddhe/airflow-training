docker build --no-cache -t python_load .
mkdir -p app/data/to_ingest/bronze
mkdir -p app/data/to_ingest/silver
docker run --rm -it \
	-e HOST=postgres -e DATABASE=airflow -e USER=airflow -e PASSWORD=airflow \
	--network github-airflow_default -v $PWD/app:/app --name python_load python_load bash