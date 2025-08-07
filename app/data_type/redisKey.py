import time
import heapq
from collections import deque
from typing import Optional, List

class Entry:
    def __init__(self, kind, value, expire_at=None):
        self.kind = kind
        self.value = value
        self.expire_at = expire_at

class RedisKey:
    def __init__(self):
        self._data = {}  # key -> Entry
        self._expires = []  # heap of (expire_at, key)
        self._transaction_queue = []  # List of [key, value, ttl, kind] for transaction updates

    def _evict_expired(self):
        now = time.time()
        while self._expires and self._expires[0][0] <= now:
            exp, key = heapq.heappop(self._expires)
            ent = self._data.get(key)
            if ent and ent.expire_at == exp:
                del self._data[key]

    def add_data(self, key: str, val, ttl: Optional[float], kind: str = None, client_state=None) -> None:
        print(f"inadd_data {key},{val}")
        try:
            expire = time.time() + ttl if ttl is not None else None
            # Determine kind if not provided
            if kind is None:
                if isinstance(val, int):
                    kind = 'int'
                elif isinstance(val, str):
                    kind = 'string'
                else:
                    kind = 'None'
            if not client_state['multi_event'].is_set():
                # Queue updates during transaction
                self._transaction_queue.append([key, val, ttl, kind])
                print(f"Transaction queue: {self._transaction_queue}, Multi: {client_state['multi_event'].is_set()}")
            else:
                # Apply immediately
                self._data[key] = Entry(kind, val, expire)
                print(f"Direct update: {key}={val}, Multi: {client_state['multi_event'].is_set()}")
                if expire:
                    heapq.heappush(self._expires, (expire, key))
        except Exception as e:
            print(f"Error in add_data: {str(e)}")
            raise

    def apply_transaction(self) -> None:
        # Apply queued transaction updates
        print(f"Applying transaction: {self._transaction_queue}")
        for key, value, ttl, kind in self._transaction_queue:
            try:
                expire = time.time() + ttl if ttl is not None else None
                self._data[key] = Entry(kind, value, expire)
                if expire:
                    heapq.heappush(self._expires, (expire, key))
            except Exception as e:
                print(f"Error applying transaction for {key}: {str(e)}")
        self._transaction_queue.clear()
        print(f"After transaction: {self._data}")
        
    def discard_transaction(self) -> None:
        # Apply queued transaction updates
        print(f"Applying transaction: {self._transaction_queue}")
        self._transaction_queue.clear()
        print(f"After transaction: {self._data}")

    def get_val(self, key: str):
        self._evict_expired()
        ent = self._data.get(key)
        if not ent:
            return None
        return ent.value

    def get_type(self, key: str) -> Optional[str]:
        self._evict_expired()
        ent = self._data.get(key)
        return ent.kind if ent else None

    def ensure_list(self, key: str):
        self._evict_expired()
        ent = self._data.get(key)
        if ent and ent.kind != 'list':
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind")
        if not ent:
            ent = Entry('list', deque(), None)
            self._data[key] = ent
        return ent.value

    def rset(self, args: List[str], client_state=None) -> None:
        print(f"inrset,{args}")
        try:
            key, val = args[0], args[1]
            ttl = float(args[3]) / 1000 if len(args) == 4 else None
            try:
                int_val = int(val)
                self.add_data(key, int_val, ttl, 'int', client_state)
            except ValueError:
                self.add_data(key, val, ttl, 'string', client_state)
        except Exception as e:
            print(f"Error in rset: {str(e)}")
            raise

    def incr(self, key: str, client_state=None) -> str:
        try:
            self._evict_expired()
            if key not in self._data:
                # Initialize non-existent key to 1
                if not client_state['multi_event'].is_set():
                    self._transaction_queue.append([key, 1, None, 'int'])
                    print(f"INCR queue: {key}=1, Multi: {client_state['multi_event'].is_set()}")
                else:
                    self._data[key] = Entry('int', 1, None)
                    print(f"INCR direct: {key}=1, Multi: {client_state['multi_event'].is_set()}")
                return ":1\r\n"
            ent = self._data[key]
            if ent.kind != 'int' and ent.kind != 'string':
                return "-ERR value is not an integer or out of range\r\n"
            try:
                int_value = int(ent.value)
            except (ValueError, TypeError):
                return "-ERR value is not an integer or out of range\r\n"
            new_value = int_value + 1
            if not client_state['multi_event'].is_set():
                self._transaction_queue.append([key, new_value, ent.expire_at if ent.expire_at else None, 'int'])
                print(f"INCR queue: {key}={new_value}, Multi: {client_state['multi_event'].is_set()}")
            else:
                self._data[key] = Entry('int', new_value, ent.expire_at)
                print(f"INCR direct: {key}={new_value}, Multi: {client_state['multi_event'].is_set()}")
                if ent.expire_at:
                    heapq.heappush(self._expires, (ent.expire_at, key))
            return f":{new_value}\r\n"
        except Exception as e:
            print(f"Error in incr: {str(e)}")
            raise

rkey = RedisKey()

def get_store():
    return rkey._data

def get(args):
    return rkey.get_val(args[0])

def rset(args, client_state=None):
    rkey.rset(args, client_state)

def check_type(key):
    return rkey.get_type(key)

def incr(item, client_state=None):
    return rkey.incr(item, client_state)