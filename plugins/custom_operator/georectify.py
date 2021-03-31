from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.dummy import DummyOperator

class GeoRectifyOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            filename: str,
            **kwargs):
        super().__init__(**kwargs)
        self.name = filename

    def execute(self, context):
        self._to_be_process()
        message = "Hello {}".format(self.name)
        print(message)
        return message
