import os
from collections import namedtuple

DriverConfig = namedtuple('DriverConfig', ['driver', 'driver_config', 'action', 'duration', 'obj_size'])

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

    def tell(self):
        return self._pos

    def close(self):
        pass

class Driver:
    def setup(self):
        raise NotImplementedError('setup has not been implemented!')

    def put(self):
        raise NotImplementedError('put has not been implemented!')

    def get(self):
        raise NotImplementedError('get has not been implemented!')

    def delete(self):
        raise NotImplementedError('delete has not been implemented!')

    def _get_bytestream(self, num_bytes):
        return FakeFile(num_bytes)
