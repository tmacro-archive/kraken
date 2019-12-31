import os
import uuid
from collections import namedtuple

from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient

from .driver import Driver

# with open(download_file_path, "wb") as download_file:
#     download_file.write(blob_client.download_blob().readall())


BlobDriverConfig = namedtuple('BlobDriverConfig', ['connection_str'])

class BlobDriver(Driver):
    def setup(self, driver_conf):
        self._client = BlobServiceClient.from_connection_string(driver_conf.connection_str)
        self._container_name = 'blobdriver-%s'%uuid.uuid4().hex
        self._container = self._client.create_container(self._container_name)

    def put(self, num_bytes):
        blob_name = 'blob-%s'%uuid.uuid4().hex
        blob_client = self._client.get_blob_client(self._container_name, blob=blob_name)
        with self._get_bytestream(num_bytes) as to_upload:
            blob_client.upload_blob(to_upload)
    
    def get(self):
        pass
