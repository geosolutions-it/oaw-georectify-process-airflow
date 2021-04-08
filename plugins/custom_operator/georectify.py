import re
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from geotiflib.georectify import GeoRectifyFactory
from geotiflib.geotiff import GeoTiff
import json


class GeoRectifyOperator(BaseOperator):
    @apply_defaults
    def __init__(self, abs_filepath: str, output_folder: str, filename:str, **kwargs):
        super().__init__(**kwargs)
        self.abs_filepath = abs_filepath
        self.output_folder = output_folder
        self.filename = filename

    def execute(self, context):
        geo_img = GeoTiff(self.abs_filepath)
        if geo_img.is_processed():
            self.log.info(f"SKIP: GeoTiff img already processed: {self.abs_filepath}")
            return self._geonode_payload()

        self.log.info(f"PROCESSING: GeoTiff img start processing: {self.abs_filepath}")
        process = GeoRectifyFactory.create(
            input=self.abs_filepath,
            qgis_scripts="/usr/bin/",
            output_folder=self.output_folder,
        )

        def on_progress(message):
            print(str(message["value"]))

        process.on_progress += on_progress
        process.process()

        self.log.info(f"PROCESSING DONE: Retrieving metadata: {self.abs_filepath}")
        
        return self._geonode_payload()

    def _geonode_payload(self):
        info = GeoTiff(self.abs_filepath).info()
        name = re.sub("\.tif$", "", self.filename)

        # TODO get required information from geotiff lib 
        # before create the json required for geonode
        geonode_json = {
            "args": [
                f"{self.output_folder}/{self.filename}",  # use {self.output_folder}
                "--overwrite",
            ],
            "kwargs": {
                "user": "admin",
                "name": name,
                "title": "Layer name",  # get from geotiff lib
                "regions": "global", # get from geotiff lib
                "keywords": "key1,key89,key344", # get from geotiff lib
            },
        }
        return json.dumps(geonode_json)