from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.dummy import DummyOperator
from geotiflib.georectify import GeoRectifyFactory
from geotiflib.geotiff import GeoTiff
class GeoRectifyOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            abs_filepath: str,
            output_folder: str,
            **kwargs):
        super().__init__(**kwargs)
        self.abs_filepath = abs_filepath
        self.output_folder = output_folder

    def execute(self, context):
        process = GeoRectifyFactory.create(
            input=self.abs_filepath, 
            qgis_scripts='/usr/bin/',
            output_folder=self.output_folder
            )
        def on_progress(message):
            print(str(message["value"]))
        process.on_progress += on_progress
        process.process()
        info = GeoTiff(self.abs_filepath).info()
        return info
