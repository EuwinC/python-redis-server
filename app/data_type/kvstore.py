import time
import heapq
from collections import deque

class Entry:
    def __init__(self, kind, value, expire_at=None):
        self.kind = kind
        self.value = value
        self.expire_at = expire_at

class KVStore:
    def __init__(self):
        self._data = {}            # key -> Entry
        self._expires = []         # heap of (expire_at, key)

    def _evict_expired(self):
        now = time.time()
        while self._expires and self._expires[0][0] <= now:
            exp, key = heapq.heappop(self._expires)
            ent = self._data.get(key)
            if ent and ent.expire_at == exp:
                del self._data[key]

    def set_string(self, key, val, ttl=None):
        expire = time.time() + ttl if ttl else None
        self._data[key] = Entry('string', val, expire)
        if expire:
            heapq.heappush(self._expires, (expire, key))

    def get_string(self, key):
        self._evict_expired()
        ent = self._data.get(key)
        if not ent or ent.kind != 'string':
            return None
        return ent.value

    def get_type(self, key):
        self._evict_expired()
        ent = self._data.get(key)
        return ent.kind if ent else None

    def ensure_list(self, key):
        self._evict_expired()
        ent = self._data.get(key)
        if ent and ent.kind != 'list':
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind")
        if not ent:
            ent = Entry('list', deque(), None)
            self._data[key] = ent
        return ent.value

# then instantiate a global store
store = KVStore()

def get_store():
    return store._data

