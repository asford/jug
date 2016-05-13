""" Specialized object persistance file-descriptor based files. """

from abc import ABCMeta, abstractproperty, abstractmethod
import shutil

class BaseFileManager(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def can_load(self, file):
        """True if manage can decode the contents of the given file."""
        raise NotImplementedError("can_decode")

    @abstractmethod
    def load(self, file):
        """Decode object from file."""
        raise NotImplementedError("decode")

    @abstractmethod
    def can_dump(self, obj):
        """True if encoder can encode obj to file."""
        raise NotImplementedError("can_encode")

    @abstractmethod
    def dump(self, obj, file):
        """Write obj to given file."""
        raise NotImplementedError("can_encode")

class NDArrayFileManager(BaseFileManager):
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

class H5PyFileManager(BaseFileManager):
    max_userblock_size = 4096

    @classmethod
    def can_load(cls, file):
        try:
            return detect_hdf5_signature( file, cls.max_userblock_size )
        finally:
            file.seek(0)

    @classmethod
    def load(cls, file):
        return h5py.File( file.name, mode="r" )

    @classmethod
    def can_dump(cls, obj):
        if type(obj) == h5py.File:
            return True

    @classmethod
    def dump(cls, obj, file):
        obj.flush()

        with open(obj.filename, "r") as infile:
            shutil.copyfileobj(infile, file)

file_managers = []

try:
    import numpy
    file_managers.append( NDArrayFileManager() )
except ImportError:
    pass

try:
    from .h5py_support import detect_hdf5_signature
    import h5py

    file_managers.append( H5PyFileManager() )
except ImportError:
    pass
