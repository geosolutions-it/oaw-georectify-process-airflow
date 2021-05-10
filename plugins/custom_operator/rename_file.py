import json
import os
import shutil
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class FileRenameOperator(BaseOperator):
    template_fields = ["prev_xcom"]

    @apply_defaults
    def __init__(
        self,
        prev_xcom: str,
        file_to_upload: str,
        output_dir: str,
        filename: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.prev_xcom = prev_xcom
        self.filename = filename
        self.file_to_upload = file_to_upload
        self.output_dir = output_dir

    def execute(self, context):

        xcom = json.loads(self.prev_xcom)
        bibid = xcom.get('typename')

        f = os.path.exists(f"{self.output_dir}/{self.filename}")
        new_file = f"{self.output_dir}/{bibid}.tif"
        _file = f"{self.output_dir}/{self.filename}"

        if f"{bibid}.tif" == self.filename:
            self.log.info("Filename is already set as BIBID code. Skipping.....")
            xcom['updated_filename'] = f"{self.output_dir}/{self.filename}" if f else new_file
            return json.dumps(xcom)


        self.log.info(f"Filename is not set as BIBID code. Starting rename process: {self.filename}")

        if not f:
            self.log.info(f"Image is not in the output path, Start coping and renaming: {self.file_to_upload} into: {new_file}")
            _file = self.file_to_upload
            shutil.copy2(_file, new_file)
            xcom['updated_filename'] = new_file
            self.log.info(f"Rename completed, pushing XCOM")
            return json.dumps(xcom)

        self.log.info(f"Img is in the output path. start renaming from: {_file} into: {new_file}")

        os.rename(f"{self.output_dir}/{self.filename}", new_file)
        xcom['updated_filename'] = new_file
        self.log.info(f"Rename completed, pushing XCOM")
        return json.dumps(xcom)

