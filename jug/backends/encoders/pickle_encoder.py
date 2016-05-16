import logging
logger = logging.getLogger(__name__)

from .base import BaseEncoder
from six.moves import cPickle as pickle
import pickletools

class PickleEncoder(BaseEncoder):

    @classmethod
    def can_load(cls, file):
        opt_bytes = file.read(2)
        first_opt = pickletools.code2op.get(opt_bytes[0], None)

        if first_opt and first_opt.name == "PROTO":
            version = ord(opt_bytes[1])
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
