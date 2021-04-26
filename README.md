# oaw-georectify-process-airflow
Airflow based project to manage the geoprectify process


# Start up the project:

Copy `env.sample` and edit `.env`:

```
cp env.sample .env
```

First time only:

```
mkdir ./logs
#adjust to UID/GID to be used by airflow configured in `.env`
sudo chown 50000:50000 ./logs
sudo chown 50000:50000 ./output
docker build --no-cache -t custom/airflow:latest .
docker network create oaw_geonode_default
docker-compose up airflow-init
docker-compose up
```

Other times:

Application run
```
docker-compose up
```
Rebuild image:
```
docker build --no-cache -t custom/airflow:latest .
```


# Import project variables:

```
docker-compose run airflow-worker variables import /opt/settings/variables.json
```

# Log into docker container

Use the `.sh` file plus the airflow cli commands:

Example:

```
./airflow.sh info

./airflow.sh bash
```
