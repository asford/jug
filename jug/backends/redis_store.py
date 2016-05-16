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
redis_store: store based on a redis backend
'''


import re
import logging
logger = logging.getLogger("jug")

from base64 import b64encode, b64decode

from jug.backends.encode import encode, decode
from .base import base_store, base_lock


try:
    import redis
    redis_functional = True
except ImportError:
    redis = None
    redis_functional = False

_LOCKED = 1

_redis_urlpat = re.compile(r'redis://(?P<host>[A-Za-z0-9\.\-]+)?(\:(?P<port>[0-9]+))?/?(?P<prefix>.+)?')


class redis_store(base_store):
    def __init__(self, url):
        '''
        '''
        if redis is None:
            raise IOError('jug.redis_store: redis module is not found!')
        self.redis_params = {}
        match = _redis_urlpat.match(url)
        params = match.groupdict()
        logger.info('Parsed %r params: %s' % (url, params))
        
        if params.get("host", None):
            self.redis_params["host"] = params["host"]
            
        if params.get("port", None):
            self.redis_params["port"] = int(params["port"])
        
        if params.get("prefix", ""):
            self.prefix = params.get("prefix", "") + "/"
        else:
            self.prefix = "/"
        self.redis = redis.Redis(**self.redis_params)
        
        logging.info("Loaded: %s", self.redis) 
        logging.info("Prefix: %r", self.prefix)
        
    def redis_key(self, key_type, name):
        return (key_type + ":" + self.prefix + name).encode("utf8")

    def _resultname(self, name):
        return self.redis_key("result", name)
    
    def _lockname(self, name):
        return self.redis_key("lock", name)

    def dump(self, object, name):
        '''
        dump(object, name)
        '''
        s = encode(object)
        if s:
            s = b64encode(s)
        self.redis.set(self._resultname(name), s)


    def can_load(self, name):
        '''
        can = can_load(name)
        '''
        return self.redis.exists(self._resultname(name))


    def load(self, name):
        '''
        obj = load(name)

        Loads the object identified by `name`.
        '''
        s = self.redis.get(self._resultname(name))
        if s:
            s = b64decode(s)
        return decode(s)


    def remove(self, name):
        '''
        was_removed = remove(name)

        Remove the entry associated with name.

        Returns whether any entry was actually removed.
        '''
        return self.redis.delete(self._resultname(name))


    def cleanup(self, active):
        '''
        cleanup()

        Implement 'cleanup' command
        '''
        existing = list(self.list())
        for act in active:
            try:
                existing.remove(self._resultname(act))
            except KeyError:
                pass
        for superflous in existing:
            self.redis.delete(self._resultname(superflous))

    def remove_locks(self):
        locks = self.redis.keys('lock:*')
        for lk in locks:
            self.redis.delete(lk)
        return len(locks)

    def list(self):
        prefix = self.redis_key("result", "")
        existing = self.redis.keys(prefix + "*")
        
        for ex in existing:
            yield ex[len(prefix):]

    def listlocks(self):
        prefix = self.redis_key("result", "")
        existing = self.redis.keys(prefix + "*")
        
        for ex in existing:
            yield ex[len(prefix):]
            
    def getlock(self, name):
        return redis_lock(self.redis, self._lockname(name))

    def close(self):
        # It seems some versions of the protocol are implemented differently
        # and do not have the ``disconnect`` method
        try:
            self.redis.disconnect()
        except:
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
        '''
        lock.get()
        '''
        previous = self.redis.getset(self.lockname, _LOCKED)
        return (previous is None)


    def release(self):
        '''
        lock.release()

        Removes lock
        '''
        self.redis.delete(self.lockname)


    def is_locked(self):
        '''
        locked = lock.is_locked()
        '''
        status = self.redis.get(self.lockname)
        return status is not None and status == _LOCKED

