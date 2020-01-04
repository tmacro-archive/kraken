import os
import uuid
from collections import namedtuple

from boto3 import Session

from .base import Driver

S3DriverConfig = namedtuple('S3DriverConfig', ['access_key', 'secret_key', 'endpoint'])

class S3Driver(Driver):
    def _setup(self, driver_conf):
        self._client = Session(aws_access_key_id = driver_conf.access_key,
                    aws_secret_access_key=driver_conf.secret_key,
                ).resource('s3', endpoint_url=driver_conf.endpoint)
        try:
            self._client.Bucket(self._bucket).create()
        except Exception:
            pass

    def _put(self, bucket, key, data):
        s3_client = self._client.Object(bucket, key)
        s3_client.upload_fileobj(data)
        return True
    
    def _get(self, bucket, key, output):
        s3_client = self._client.Object(bucket, key)
        s3_client.download_fileobj(output)
        return True
