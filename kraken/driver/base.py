import os
import uuid
from collections import namedtuple
from datetime import datetime, timezone

from ..utils import Timer, instrument_call


class FakeFile:
    def __init__(self, size, content = b'0'):
        self._size = size
        self._pos = 0
        self._content = bytes(content)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def _makebytes(self, num):
        self._pos += num
        return self._content * num

    def read(self, num = -1):
        if num == -1:
            return self._makebytes(self._size)
        if self._pos == self._size:
            return self._makebytes(0)
        elif self._size < self._pos + num:
            return self._makebytes(self._size - self._pos)
        return self._makebytes(num)

    def seek(self, offset, from_what = os.SEEK_SET):
        if from_what == os.SEEK_SET:
            self._pos = offset
        elif from_what == os.SEEK_CUR:
            self._pos += offset
        elif from_what == os.SEEK_END:
            self._pos = self._size - offset
        return self._pos

    def write(self, data):
        pass

    def tell(self):
        return self._pos

    def close(self):
        pass

class Driver:
    def __init__(self):
        self._bucket = None
        self._key_prefix = None
        self._key_start= None
        self._key_step = None

    def _get_bytestream(self, num_bytes):
        return FakeFile(num_bytes)

    def _get_key(self):
        for i in range(self._key_start, 1000000, self._key_step):
            yield '%s-%d'%(self._key_prefix, i)

    def setup(self, config):
        self._bucket = config.bucket
        self._key_prefix = '%s-%s'%(config.key_prefix, uuid.uuid4().hex)
        self._key_start = config.key_start
        self._key_step = config.key_step
        self._object_size = config.obj_size
        self._duration = config.duration
        self._setup(config.driver.config)
    
    def _setup(self, driver_conf):
        raise NotImplementedError('setup has not been implemented!')

    def put(self):
        timer = Timer(self._duration)
        for key in self._get_key():
            if not timer():
                break
            yield instrument_call(self._put, self._bucket, key, FakeFile(self._object_size))._replace(type='put', key=key, bucket=self._bucket)

    def _put(self, bucket, key, data):
        raise NotImplementedError('_put has not been implemented!')

    def get(self):
        timer = Timer(self._duration)
        for key in self._get_key():
            if not timer():
                break
            yield instrument_call(self._get, self._bucket, key, FakeFile(0))._replace(type='get', key=key, bucket=self._bucket)

    def _get(self, bucket, key, output):
        raise NotImplementedError('_get has not been implemented!')

    def delete(self, bucket, key):
        return self._delete(bucket, key)

    def _delete(self, bucket, key):
        raise NotImplementedError('_delete has not been implemented!')
