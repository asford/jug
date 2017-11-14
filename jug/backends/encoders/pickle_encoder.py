import logging
logger = logging.getLogger(__name__)

from .base import BaseEncoder
from six.moves import cPickle as pickle

class PickleEncoder(BaseEncoder):

    @classmethod
    def can_load(cls, file):
        opt_bytes = file.peek(2)
        assert(isinstance(opt_bytes, bytes))

        if opt_bytes[0] == b'\x80'[0]:
            version = opt_bytes[1]
            if version <= pickle.HIGHEST_PROTOCOL:
                return True
            else:
                logger.warning("Found PROTO opt with pickle version %s > pickle.HIGHEST_PROTOCOL.", version)
                return False
        else:
            return False

    @classmethod
    def load(cls, file):
        return pickle.load(file)

    @classmethod
    def can_dump(cls, obj):
        return True

    @classmethod
    def dump(cls, obj, file):
        pickle.dump(obj, file, pickle.HIGHEST_PROTOCOL)
