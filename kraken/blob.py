import os
import uuid
from collections import namedtuple

from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from azure.core.exceptions import ResourceExistsError
from .driver import Driver

# with open(download_file_path, "wb") as download_file:
#     download_file.write(blob_client.download_blob().readall())


BlobDriverConfig = namedtuple('BlobDriverConfig', ['connection_str'])

class BlobDriver(Driver):
    def _setup(self, driver_conf):
        self._client = BlobServiceClient.from_connection_string(driver_conf.connection_str)
        try:
            self._client.create_container(self._bucket)
        except ResourceExistsError:
            pass

    def _put(self, container, key, data):
        blob_client = self._client.get_blob_client(container, blob=key)
        blob_client.upload_blob(data)
        return True
    
    def _get(self):
        pass
