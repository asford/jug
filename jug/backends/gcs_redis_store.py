#-*- coding: utf-8 -*-
# Copyright (C) 2009-2011, Luis Pedro Coelho <luis@luispedro.org>
# vim: set ts=4 sts=4 sw=4 expandtab smartindent:
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

'''
gcs_redis_store: A hybrid store storing results in GCS and metadata in redis.
'''


import logging
logger = logging.getLogger(__name__)

import re
import socket
import os

from jug.backends.encode import encode, decode
from .base import base_store, base_lock


try:
    import redis
    import gcloud.storage
    _import_error = None
except ImportError as e:
    redis = None
    gcloud = None
    _import_error = e

_LOCKED = 1

store_connection_pattern  = re.compile(
    r"gcs\+redis://"
    r"(?P<bucket>[A-Za-z0-9\.\-]+)"
    r"\+(?P<redis_host>[A-Za-z0-9\.\-]+)(\:(?P<port>[0-9]+))?"
    r"/?(?P<prefix>.+)?"
)

def _delete_keys_script(key_pattern):
    """Return script deleting keys matching given pattern for use with redis exec."""
    return """
        local curkey = redis.call('keys', '%(key_pattern)s')
        if next(curkey) then
            redis.call('del', unpack(curkey))
        end
    """ % dict(key_pattern = key_pattern)


class gcs_redis_store(base_store):
    def __init__(self, connection_string):
        if _import_error:
            logger.error("Unable to import gcloud or redis modules.")
            raise _import_error

        self.redis_params = {}
        match = store_connection_pattern.match(connection_string)
        if not match:
            raise ValueError("Unable to parse gcs_redis connection string: %r", connection_string)

        params = match.groupdict()
        logger.info('Parsed %r params: %s' % (connection_string, params))
        
        self.bucket_name = params["bucket"]
        self.client = gcloud.storage.Client()
        self.bucket = self.client.get_bucket(self.bucket_name)

        self.redis_params["host"] = params["redis_host"]
        if params.get("port", None):
            self.redis_params["port"] = int(params["port"])

        if params.get("prefix", ""):
            self.prefix = params.get("prefix", "") + "/"
        else:
            self.prefix = "/"
        self.redis = redis.Redis(**self.redis_params)
        
        logging.info("bucket: %s", self.bucket)
        logging.info("redis: %s", self.redis) 
        logging.info("prefix: %r", self.prefix)
        
    def storage_key(self, name):
        return self.prefix + name

    def redis_key(self, key_type, name):
        return (key_type + ":" + self.prefix + name).encode("utf8")

    def redis_result_key(self, name):
        return self.redis_key("gcsresult", name)
    
    def redis_lock_key(self, name):
        return self.redis_key("lock", name)

    def dump(self, obj, name):
        '''
        dump(object, name)
        '''
        s = encode(obj)
        sblob = self.bucket.blob(self.storage_key(name))
        sblob.upload_from_string(s, content_type=None)
        self.redis.set(self.redis_result_key(name), self.storage_key(name))

    def can_load(self, name):
        '''
        can = can_load(name)
        '''
        return self.redis.exists(self.redis_result_key(name))

    def load(self, name):
        '''
        obj = load(name)

        Loads the object identified by `name`.
        '''

        s = self.bucket.blob(self.storage_key(name)).download_as_string()
        return decode(s)

    def remove(self, name):
        '''
        was_removed = remove(name)

        Remove the entry associated with name.

        Returns whether any entry was actually removed.
        '''
        removed = self.redis.delete(self.redis_result_key(name))
        try:
            self.bucket.blob(self.storage_key(name)).delete()
        except:
            logging.exception("Error removing storage blob: %s" % self.storage_key)
        return removed

    def sync(self):
        current_keys = self.redis.keys(self.redis_result_key("*"))
        current_blobs = list(self.bucket.list_blobs(prefix=self.prefix))

        logger.info("sync gcs objects: %s redis markers: %s", len(current_blobs), len(current_keys))

        sync_script = "\n".join(
            [_delete_keys_script(self.redis_result_key("*"))] +
            [
                "redis.call('set', '%s', '%s')"
                 % (self.redis_result_key(b.name[len(self.prefix):]), b.name)
                 for b in current_blobs
            ]
        )

        self.redis.eval(sync_script, 0)

    def cleanup(self, active):
        '''
        cleanup()

        Implement 'cleanup' command
        '''
        # Begin by hard-syncing redis result state with gcs backend.
        self.sync()
        
        existing = list(self.list())

        for inactive in set(existing) - set(active):
            self.remove(inactive)

    def remove_locks(self):
        locks = self.redis.keys(self.redis_lock_key('*'))
        for lk in locks:
            self.redis.delete(lk)
        return len(locks)

    def list(self):
        prefix = self.redis_result_key("")
        existing = self.redis.keys(prefix + "*")
        
        for ex in existing:
            yield ex[len(prefix):]

    def listlocks(self):
        prefix = self.redis_lock_key("")
        existing = self.redis.keys(prefix + "*")
        
        for ex in existing:
            yield ex[len(prefix):]
            
    def getlock(self, name):
        return redis_lock(self.redis, self.redis_lock_key(name))

    def close(self):
        pass

class redis_lock(base_lock):
    '''
    redis_lock

    Functions:
    ----------

        * get(): acquire the lock
        * release(): release the lock
        * is_locked(): check lock state
    '''

    def __init__(self, redis, lockname):
        self.redis = redis
        self.lockname = lockname

    def get(self):
        lock_contents = "%s:%s" % (socket.gethostname(), os.getpid())
        acquire_lock = self.redis.set(self.lockname, lock_contents, nx=True)
        return acquire_lock


    def release(self):
        self.redis.delete(self.lockname)


    def is_locked(self):
        return self.redis.get(self.lockname) is not None
