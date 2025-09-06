docker build --no-cache -t python_transform .
mkdir -p app/data/to_ingest/bronze
mkdir -p app/data/to_ingest/silver
docker run --rm -it -v $PWD/app:/app --name python_transform python_transform bash