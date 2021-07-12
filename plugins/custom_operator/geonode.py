from io import BufferedReader, IOBase
import os
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import time
import json
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from requests.models import HTTPBasicAuth
from json2xml import json2xml
from json2xml.utils import readfromstring

class GeoNodeUploaderOperator(BaseOperator):
    template_fields = ["custom_metadata"]

    @apply_defaults
    def __init__(
        self,
        custom_metadata: dict,
        connection: str,
        call_delay: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.custom_metadata = custom_metadata
        self.connection = connection
        self.call_delay = call_delay
        self.filename = None

    def execute(self, context):
        _file = json.loads(self.custom_metadata).pop('updated_filename')
        self.filename = os.path.basename(_file)

        base, ext = os.path.splitext(_file)
        self.log.info(f"Metadata extracted: {self.custom_metadata}")

        data = readfromstring(self.custom_metadata)

        self.log.info(f"Saving Metadata in temp file: {self.filename}")

        with open(f"/tmp/{self.filename}.xml", 'w+') as meta:
                meta.write(json2xml.Json2xml(data).to_xml())

        self.log.info(f"Metadata Converted: {json2xml.Json2xml(data).to_xml()}")

        params = {
            "permissions": '{ "users": {"AnonymousUser": ["view_resourcebase"]} , "groups":{}}',
            "time": "false",
            "charset": "UTF-8",
        }

        if ext.lower() == ".tif":
            file_path = base + ext
            params["tif_file"] = open(file_path, "rb")

        params['xml_file'] = open(f"/tmp/{self.filename}.xml", 'rb')

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
                f"Sending PUT request to geonode: http://{conn.host}:{conn.port}/api/v2/uploads/upload/"
            )

            response = client.put(
                f"http://{conn.host}:{conn.port}/api/v2/uploads/upload/",
                auth=HTTPBasicAuth(conn.login, conn.password),
                data=params,
                files=files,
            )

            self.log.info(f"Geonode response with status code {response.status_code}")

        self.log.info("Closing spatial files")

        if isinstance(params.get("tif_file"), IOBase):
            params["tif_file"].close()

        if isinstance(params.get("xml_file"), IOBase):
            params["xml_file"].close()

        if response.status_code != 201:
            raise Exception("An error has occured with the communication with GeoNode: {response.json()}")

        self.log.info("Getting import_id")
        time.sleep(self.call_delay)                
        import_id = int(response.json()["redirect_to"].split("?id=")[1])
        self.log.info(f"ImportID found with ID: {import_id}")

        time.sleep(self.call_delay)
        self.log.info(f"Getting upload_list")
        upload_response = client.get(f"http://{conn.host}:{conn.port}/api/v2/uploads/")

        self.log.info(f"Extraction of upload_id")

        upload_id = self._get_upload_id(upload_response, import_id)

        self.log.info(f"UploadID found {upload_id}")

        self.log.info(f"Calling upload detail page")
        client.get(f"http://{conn.host}:{conn.port}/api/v2/uploads/{upload_id}")

        time.sleep(self.call_delay)
        self.log.info(f"Calling final upload page")
        client.get(f"http://{conn.host}:{conn.port}/upload/final?id={import_id}")

        self.log.info(f"Layer added in GeoNode")

        self.log.info(f"Checking upload status")

        response = self.check_status(client, upload_id, conn)

        self.log.info("Upload process completed")
        return response

    def check_status(self, client, upload_id, conn):
        r = client.get(f"http://{conn.host}:{conn.port}/api/v2/uploads/{upload_id}")
        state = r.json()["upload"]["state"]

        if state in ['INVALID', 'PENDING', 'WAITING']:
            raise AirflowException("Some error has occured during upload, please check the logs")

        if state != "PROCESSED":
            self.log.info("Process not finished yet, waiting")
            time.sleep(self.call_delay)
            self.check_status(client, upload_id, conn)
        return r.json()

    @staticmethod
    def _get_upload_id(upload_response, import_id):
        for item in upload_response.json()["uploads"]:
            if item.get("import_id", None) == import_id:
                return item.get("id", None)

    def _get_connection(self):
        return BaseHook.get_connection(self.connection)
