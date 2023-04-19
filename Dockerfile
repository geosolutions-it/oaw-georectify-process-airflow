FROM apache/airflow:2.0.1
COPY requirements.txt /opt
USER root
RUN apt-get update && apt-get install qgis -y
RUN apt-get install -y build-essential gdal-bin \
    libxml2 libxml2-dev gettext \
    libxslt1-dev libjpeg-dev libpng-dev libpq-dev libgdal-dev \
    software-properties-common build-essential \
    git unzip gcc zlib1g-dev libgeos-dev libproj-dev binutils python3-gdal mlocate
RUN pip install pygdal=="`gdal-config --version`.*"
RUN pip install --no-cache-dir -r /opt/requirements.txt
