from .base import BaseEncoder
import io
import numpy

class NDArrayEncoder(BaseEncoder):
    @classmethod
    def can_load(cls, file):
        test_bytes = file.peek(1024)
        try:
            magic = numpy.lib.npyio.format.read_magic(io.BytesIO(test_bytes))
            if magic:
                return True
        except ValueError:
            pass
        finally:
            file.seek(0)

        return False

    @classmethod
    def load(cls, file):
        return numpy.lib.npyio.format.read_array(file)

    @classmethod
    def can_dump(cls, obj):
        return type(obj) == numpy.ndarray

    @classmethod
    def dump(cls, obj, file):
        numpy.lib.npyio.format.write_array(file, obj)
