"""Abstract base for jug object encoders."""

from abc import ABCMeta, abstractproperty, abstractmethod
class BaseEncoder(object):
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
