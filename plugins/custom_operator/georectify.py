from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from geotiflib.georectify import GeoRectifyFactory
from geotiflib.geotiff import GeoTiff
from airflow.models.variable import Variable
import json
import ast


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
            min_points=ast.literal_eval(Variable.get('MIN_POINTS', "-1"))
        )

        def on_progress(message):
            print(str(message["value"]))

        process.on_progress += on_progress
        process.process()

        self.log.info(f"PROCESSING DONE: Retrieving metadata: {self.abs_filepath}")
        
        return self._geonode_payload()

    @staticmethod
    def _get_attribute_value(metadata, attribute):
        value = metadata.get(attribute, None)
        if value is None:
            raise AttributeError(f"'{attribute}' metadata attribute is missing!")
        return value

    def _geonode_payload(self):
        metadata = GeoTiff(self.abs_filepath).oaw_metadata_dict()
        keywords = list(set([k.replace(' ', '') for k in self._get_attribute_value(metadata, 'subject').replace("&amp;", '&').replace("|", ";").split(';')]))
        geonode_json = {
                "title": self._get_attribute_value(metadata, 'title').replace("+", " "),
                "date": self._get_attribute_value(metadata, 'date'),
                "edition":  self._get_attribute_value(metadata, 'edition'),
                "abstract": self._get_attribute_value(metadata, 'description').replace("+", " "),
                #"purpose": metadata.get('source', None),
                "keywords": keywords,
                "supplemental_information":  self._get_attribute_value(metadata, 'source').replace("+", " "),
                "data_quality_statement": self._get_attribute_value(metadata, 'format').replace("+", " "),
                "typename": self._get_attribute_value(metadata, 'identifier'),
            }
        return json.dumps(geonode_json)
