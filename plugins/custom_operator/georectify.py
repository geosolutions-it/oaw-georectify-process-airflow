from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from geotiflib.georectify import GeoRectifyFactory
from geotiflib.geotiff import GeoTiff
from airflow.models.variable import Variable
import codecs
from urllib.parse import unquote
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
    def _get_attribute_value(metadata, attribute, to_unquote=True, is_required=True):
        value = metadata.get(attribute, None)
        if value is None and is_required:
            raise AttributeError(f"'{attribute}' metadata attribute is missing!")
        elif value is None:
            return ""
        value = unquote(value)
        value = codecs.escape_decode(value)[0].decode()
        if not to_unquote:
            value = value.replace("&amp;", "&")
        return value

    def _geonode_payload(self):
        metadata = GeoTiff(self.abs_filepath).oaw_metadata_dict()
        keywords = list(
            set(
                [
                    k.replace(" ", "")
                    for k in self._get_attribute_value(
                        metadata, "subject", to_unquote=False, is_required=self._is_required_field('subject')
                    )
                    .replace("&amp;", "&")
                    .replace("|", ";")
                    .split(";")
                ]
            )
        )
        geonode_json = {
                "title": self._get_attribute_value(metadata, 'title', is_required=self._is_required_field('title')).replace("+", " "),
                "date": self._get_attribute_value(metadata, 'date', is_required=self._is_required_field('date')),
                "edition":  self._get_attribute_value(metadata, 'edition', is_required=self._is_required_field('edition')),
                "abstract": self._get_attribute_value(metadata, 'description', is_required=self._is_required_field('description')).replace("+", " "),
                #"purpose": metadata.get('source', None),
                "keywords": keywords,
                "supplemental_information":  self._get_attribute_value(metadata, 'source', is_required=self._is_required_field('source')).replace("+", " "),
                "data_quality_statement": self._get_attribute_value(metadata, 'format', is_required=self._is_required_field('format')).replace("+", " "),
                "typename": self._get_attribute_value(metadata, 'identifier', is_required=self._is_required_field('identifier')),
            }
        return json.dumps(geonode_json)

    @staticmethod
    def _is_required_field(field):
        required_fields = ast.literal_eval(Variable.get('OPTIONAL_METADATA', "['edition']"))
        return field not in required_fields