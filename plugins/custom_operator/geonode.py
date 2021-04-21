from io import BufferedReader, IOBase
import os
import re
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import time
from airflow.hooks.base_hook import BaseHook
from requests.models import HTTPBasicAuth


class GeoNodeUploaderOperator(BaseOperator):
    template_fields = ["custom_metadata"]

    @apply_defaults
    def __init__(
        self,
        file_to_upload: str,
        custom_metadata: dict,
        connection: str,
        filename: str,
        output_dir: str,
        call_delay: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.file_to_upload = file_to_upload
        self.custom_metadata = custom_metadata
        self.connection = connection
        self.call_delay = call_delay
        self.output_dir = output_dir
        self.filename = filename

    def execute(self, context):
        self.log.info(f"Starting upload for file: {self.output_dir}/{self.filename}")
        _file = f"{self.output_dir}/{self.filename}"
        self.log.info(f"Checking if the img is available in the output path: {_file}")

        x = os.path.exists(f"{self.output_dir}/{self.filename}")
        if not x:
            self.log.info(f"Image already processed, taking input path: {self.file_to_upload}")
            _file = self.file_to_upload

        base, ext = os.path.splitext(_file)
        params = {
            # make public since wms client doesn't do authentication
            "permissions": '{ "users": {"AnonymousUser": ["view_resourcebase"]} , "groups":{}}',  # to be decided
            "time": "false",
            "layer_title": re.sub("\.tif$", "", self.filename),
            "time": "false",
            "charset": "UTF-8",
        }

        if ext.lower() == ".tif":
            file_path = base + ext
            params["tif_file"] = open(file_path, "rb")
        self.log.info(f"Generating params dict: {params}")

        files = {}

        self.log.info("Opening client session")

        client = requests.session()
        conn = self._get_connection()
        self.log.info("Opening Files")
        with open(_file, "rb") as base_file:
            params["base_file"] = base_file
            for name, value in params.items():
                if isinstance(value, BufferedReader):
                    files[name] = (os.path.basename(value.name), value)
                    params[name] = os.path.basename(value.name)

            self.log.info(
                f"Sending PUT request to geonode: http://{conn.host}/api/v2/uploads/upload/"
            )

            response = client.put(
                f"http://{conn.host}/api/v2/uploads/upload/",
                auth=HTTPBasicAuth(conn.login, conn.password),
                data=params,
                files=files,
            )

            self.log.info(f"Geonode response with status code {response.status_code}")

        self.log.info("Closing spatial files")

        if isinstance(params.get("tif_file"), IOBase):
            params["tif_file"].close()

        self.log.info("Getting import_id")
        import_id = int(response.json()["redirect_to"].split("?id=")[1])
        self.log.info(f"ImportID found with ID: {import_id}")

        self.log.info(f"Getting upload_list")
        upload_response = client.get(f"http://{conn.host}/api/v2/uploads/")

        self.log.info(f"Extraction of upload_id")

        upload_id = self._get_upload_id(upload_response, import_id)

        self.log.info(f"UploadID found {upload_id}")

        self.log.info(f"Calling upload detail page")
        client.get(f"http://{conn.host}/api/v2/uploads/{upload_id}")

        self.log.info(f"Calling final upload page")
        client.get(f"http://{conn.host}/upload/final?id={import_id}")

        self.log.info(f"Layer added in GeoNode")

        self.log.info(f"Checking upload status")

        return self.check_status(client, upload_id, conn)

    def check_status(self, client, upload_id, conn):
        r = client.get(f"http://{conn.host}/api/v2/uploads/{upload_id}")
        print(r.json())
        state = r.json()["upload"]["state"]
        if state != "PROCESSED":
            self.log.info("Process not finished yet, waiting")
            time.sleep(self.call_delay)
            self.check_status(client, upload_id, conn)
        self.log.info("Upload process completed")
        return r.json()

    @staticmethod
    def _get_upload_id(upload_response, import_id):
        for item in upload_response.json()["uploads"]:
            if item.get("import_id", None) == import_id:
                return item.get("id", None)

    def _get_connection(self):
        return BaseHook.get_connection(self.connection)
