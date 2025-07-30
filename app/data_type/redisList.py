from collections import deque
import asyncio

class Redis_List:
    
    def __init__(self,name):
        self.name = name
        self.elements = deque()
        self._waiters = deque() #[time:action]


    def __len__(self) -> int:
        return len(self.elements)

    
    def get_name(self) -> str:
        return self.name

    def get_elements(self,i =  None) -> str:
        if i is None:
            return self.elements
        return self.elements[i]
    
    def get_element_length(self) -> int:
        return len(self.elements)
        
    def append_right(self,elements) -> None:
        self.elements.append(elements)
        if self._waiters:
            fut = self._waiters.popleft()
            if not fut.done():
                fut.set_result(elements)

    
    def append_left(self,elements:str) -> None:
        self.elements.appendleft(elements)
        if self._waiters:
            fut = self._waiters.popleft()
            if not fut.done():
                fut.set_result(elements)

    
    def pop_left(self) -> str:
        remove = self.elements.popleft()
        return remove
    
    async def blpop(self, timeout: float):
        # immediate pop if we already have data:
        if self.elements:
            return self.elements.popleft()

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._waiters.append(fut)

        try:
            if timeout > 0:
                return await asyncio.wait_for(fut, timeout)
            else:
                # block forever
                return await fut
        except asyncio.TimeoutError:
            # timed out, remove our waiter
            self._waiters.remove(fut)
            return None
        
lists: dict[str, Redis_List] = {}
# convenience API
def get_list(key: str) -> Redis_List:
    return lists.setdefault(key, Redis_List(key))

def check_if_lists(key):
    return key in lists 

def rpush(key: str, *vals: str) -> int:
    lst = get_list(key)
    for v in vals:
        lst.append_right(v)
    return len(lst)

def lpush(key: str, *vals: str) -> int:
    lst = get_list(key)
    for v in vals:
        lst.append_left(v)
    return len(lst)

def llen(key: str) -> int:
    return len(get_list(key))

def lrange(key: str, start: int, stop: int) -> list[str]:
    lst = get_list(key)
    L = len(lst)
    # normalize negatives
    if start < 0: start = max(0, L + start)
    if stop < 0: stop = L + stop
    if start > stop: return []
    stop = min(stop, L-1)
    return [lst.get_elements(i) for i in range(start, stop+1)]

def lpop_n(key: str, count: int) -> list[str]:
    lst = get_list(key)
    n = min(count, len(lst))
    return [lst.pop_left() for _ in range(n)]

async def blpop(key: str, timeout: float) -> tuple[str, str] | None:
    lst = get_list(key)
    val = await lst.blpop(timeout)
    return (key, val) if val is not None else None
    


    