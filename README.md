# oaw-georectify-process-airflow
Airflow based project to manage the geoprectify process


# Start up the project:

First time only:
```
docker-compose up airflow-init
docker-compose up
```
Other times:
```
docker-compose up
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