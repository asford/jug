from .base import BaseEncoder
import numpy

class NDArrayEncoder(BaseEncoder):
    @classmethod
    def can_load(cls, file):
        try:
            magic = numpy.lib.npyio.format.read_magic(file)
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
