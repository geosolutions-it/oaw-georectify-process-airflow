import re
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from geotiflib.georectify import GeoRectifyFactory
from geotiflib.geotiff import GeoTiff


class GeoRectifyOperator(BaseOperator):
    @apply_defaults
    def __init__(self, abs_filepath: str, output_folder: str, filename:str, **kwargs):
        super().__init__(**kwargs)
        self.abs_filepath = abs_filepath
        self.output_folder = output_folder
        self.filename = filename

    def execute(self, context):
        process = GeoRectifyFactory.create(
            input=self.abs_filepath,
            qgis_scripts="/usr/bin/",
            output_folder=self.output_folder,
        )

        def on_progress(message):
            print(str(message["value"]))

        process.on_progress += on_progress
        process.process()
        info = GeoTiff(self.abs_filepath).info()
        name = re.sub("\.tif$", "", self.filename)

        geonode_json = {
            "args": [
                f"{self.output_folder}/{self.filename}",
                "--overwrite",
            ],
            "kwargs": {
                "user": "admin",
                "name": name,
                "title": "imported layer updated",  # get from geotiff lib
                "regions": "global", # get from geotiff lib
                "keywords": "key89,key344", # get from geotiff lib
            },
        }
        return geonode_json
