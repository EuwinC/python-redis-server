import time
import heapq
from collections import deque



class Entry:
    def __init__(self, kind, value, expire_at=None):
        self.kind = kind
        self.value = value
        self.expire_at = expire_at

class RedisKey:
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

    def add_data(self, key, val, ttl=None):
        expire = time.time() + ttl if ttl else None
        if isinstance(val,int):
            self._data[key] = Entry('int', val, expire)
        elif isinstance(val,str):
            self._data[key] = Entry('string', val, expire)
        else:
            self._data[key] = Entry('None', val, expire)
        if expire:
            heapq.heappush(self._expires, (expire, key))

    
    def get_val(self, key):
        self._evict_expired()
        ent = self._data.get(key)
        if not ent:
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
rkey = RedisKey()

def get_store():
    return rkey._data

def get(args):
    return rkey.get_val(args[0])

def rset(args):
    key, val = args[0], args[1]
    
    ttl = float(args[3]) / 1000 if len(args) == 4 else None
    rkey.add_data(key, val, ttl)

def check_type(key):
    return rkey.get_type(key)
    
def incr(item) -> None:
# Check if key exists and get its value
    if item not in rkey._data:
        # Initialize non-existent key to 0, then increment to 1
        rkey.add_data(item, 1, None)
        return ":1\r\n"
    
    value = rkey._data[item].value
    
    # Try to convert value to integer
    try:
        int_value = int(value)  # Works for both int and string (e.g., '99')
    except (ValueError, TypeError):
        return "-ERR value is not an integer or out of range\r\n"
    
    # Increment and update the value
    new_value = int_value + 1
    rkey.add_data(item, new_value, rkey._data[item].expire_at)  # Preserve existing TTL
    return f":{new_value}\r\n"

