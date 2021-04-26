from io import BufferedReader, IOBase
import os
import time
import requests
from requests.models import HTTPBasicAuth
import argparse

parser=argparse.ArgumentParser()

class GeoNodeUploader():
    def __init__(
        self,
        host: str,
        folder_path: str,
        username: str,
        password: str,
        call_delay: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.host = host
        self.folder_path = folder_path
        self.username = username
        self.password = password
        self.call_delay = call_delay

    def execute(self):
        if not os.path.exists(self.folder_path):
            print("The selected path does not exist")
        
        for file in os.listdir(self.folder_path):
            print(f"Starting upload for file: {self.folder_path}/{file}")
            _file = f"{self.folder_path}/{file}"
            spatial_files = ("dbf_file", "shx_file", "prj_file")

            base, ext = os.path.splitext(_file)
            params = {
                # make public since wms client doesn't do authentication
                "permissions": '{ "users": {"AnonymousUser": ["view_resourcebase"]} , "groups":{}}',  # to be decided
                "time": "false",
                "layer_title": file,
                "time": "false",
                "charset": "UTF-8",
            }

            if ext.lower() == ".shp":
                for spatial_file in spatial_files:
                    ext, _ = spatial_file.split("_")
                    file_path = f"{base}.{ext}"
                    # sometimes a shapefile is missing an extra file,
                    # allow for that
                    if os.path.exists(file_path):
                        params[spatial_file] = open(file_path, "rb")
            elif ext.lower() == ".tif":
                file_path = base + ext
                params["tif_file"] = open(file_path, "rb")
            else:
                continue

            print(f"Generating params dict: {params}")

            files = {}

            print("Opening client session")

            client = requests.session()

            print("Opening Files")
            with open(_file, "rb") as base_file:
                params["base_file"] = base_file
                for name, value in params.items():
                    if isinstance(value, BufferedReader):
                        files[name] = (os.path.basename(value.name), value)
                        params[name] = os.path.basename(value.name)

                print(
                    f"Sending PUT request to geonode: {self.host}/api/v2/uploads/upload/"
                )

                response = client.put(
                    f"{self.host}/api/v2/uploads/upload/",
                    auth=HTTPBasicAuth(self.username, self.password),
                    data=params,
                    files=files,
                )

                print(f"Geonode response with status code {response.status_code}")

            print("Closing spatial files")

            if isinstance(params.get("tif_file"), IOBase):
                params["tif_file"].close()

            print("Getting import_id")
            import_id = int(response.json()["redirect_to"].split("?id=")[1].split("&")[0])
            print(f"ImportID found with ID: {import_id}")

            print(f"Getting upload_list")
            upload_response = client.get(f"{self.host}/api/v2/uploads/")

            print(f"Extraction of upload_id")

            upload_id = self._get_upload_id(upload_response, import_id)

            print(f"UploadID found {upload_id}")

            print(f"Calling upload detail page")
            client.get(f"{self.host}/api/v2/uploads/{upload_id}")

            print(f"Calling final upload page")
            client.get(f"{self.host}/upload/final?id={import_id}")

            print(f"Layer added in GeoNode")

            print(f"Checking upload status")

            self.check_status(client, upload_id)

    def check_status(self, client, upload_id):
        r = client.get(f"{self.host}/api/v2/uploads/{upload_id}")
        print(r.json())
        state = r.json()["upload"]["state"]
        if state != "PROCESSED":
            print("Process not finished yet, waiting")
            time.sleep(self.call_delay)
            self.check_status(client, upload_id)
        print("Upload process completed")
        return r.json()

    @staticmethod
    def _get_upload_id(upload_response, import_id):
        for item in upload_response.json()["uploads"]:
            if item.get("import_id", None) == import_id:
                return item.get("id", None)

if __name__ == "__main__":
    #getting input parameters from the userparser=argparse.ArgumentParser()

    parser.add_argument('--host', help='Example: http://localhost.com')
    parser.add_argument('--username', help='Example: foo')
    parser.add_argument('--password', help='Example: bar')
    parser.add_argument('--folder_path', help='Example: /home/user/files/')

    args=parser.parse_args()

    GeoNodeUploader(
        host=args.host,
        username=args.username,
        password=args.password,
        folder_path=args.folder_path
    ).execute()
