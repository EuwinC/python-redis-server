from collections import deque
import asyncio
class list_data:
    def __init__(self,key,value,exp_time = 0):
        self.key = key
        self.value = value
        self.exp_time = exp_time
    
    def get_value(self):
        return self.value

class Redis_List:
    
    def __init__(self,name):
        self.name = name
        self.elements = deque()
        self._waiters = deque() #[time:action]

    def get_name(self):
        return self.name

    def get_elements(self,i =  None):
        if i is None:
            return self.elements
        return self.elements[i]
    
    def get_element_length(self):
        return len(self.elements)
        
    def append_right(self,elements):
        self.elements.append(elements)
        if self._waiters:
            fut = self._waiters.popleft()
            if not fut.done():
                fut.set_result(elements)

    
    def append_left(self,elements):
        self.elements.appendleft(elements)
        if self._waiters:
            fut = self._waiters.popleft()
            if not fut.done():
                fut.set_result(elements)

    
    def pop_left(self):
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

    


    