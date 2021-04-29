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
            return self._geonode_payload(already_processed=True)

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

    def _geonode_payload(self, already_processed=False):
        metadata = GeoTiff(self.abs_filepath).oaw_metadata_dict()

        geonode_json = {
                "title": metadata.get('title', None),
                "date": metadata.get('date', None),
                "edition":  metadata.get('edition', None),
                "abstract": metadata.get('description', None),
                "purpose": metadata.get('source', None),
                "keywords": [k.replace(' ', '') for k in metadata.get('subject', None).split(';')],
                "supplemental_information":  metadata.get('relation', None),
                "data_quality_statement": metadata.get('format', None),
                "typename": metadata.get('identifier', None),
            }
        return json.dumps(geonode_json)
