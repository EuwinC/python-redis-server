from datetime import datetime
from app.database import Redis_List
import asyncio
store = dict() #[[key,value,time_delete]]
redis_list = dict() #[list_name:redis_list]
def ping_func(args):
    return "+PONG\r\n"

def echo_func(args):
    return  f"${len(args[0])}\r\n{args[0]}\r\n"

def set_func(args):
    if len(args) == 4:
        key,value,px,exp_time = args
    else:
        key,value = args
        exp_time = 1000000
    store[key] = (value,datetime.now().timestamp()+float(exp_time)/1000)
    return f"+OK\r\n"

def get_func(args):
    key = args[0]
    value,exp_time = store.get(key,"")
    if exp_time <= datetime.now().timestamp():
        del store[key]
        value = ""
    return f"${len(value)}\r\n{value}\r\n" if value != "" else "$-1\r\n"

def rpush_func(args):
    key = args[0]
    if key not in redis_list:
        redis_list[key] = Redis_List(key)
    for i in range(1,len(args)):
        redis_list[key].append_right(args[i])
    length = redis_list[key].get_element_length()
    return f":{length}\r\n" 

def lpush_func(args):
    key = args[0]
    if key not in redis_list:
        redis_list[key] = Redis_List(key)
    for i in range(1,len(args)):
        redis_list[key].append_left(args[i])
    length = redis_list[key].get_element_length()
    return f":{length}\r\n" 

def llen_func(args):
    key = args[0]
    if key not in redis_list:
        return f":0\r\n"
    length = redis_list[key].get_element_length()
    return f":{length}\r\n" 

def lrange_func(args): #array_name, left index, right index
    key = args[0]
    start, stop = int(args[1]), int(args[2])
    if key not in redis_list:
        return f"*0\r\n"
    length = redis_list[key].get_element_length()
    if start < 0:
        start = length + start
    if stop < 0:
        stop = length + stop    
    if start < 0:
        start = 0
    if start >= length or start > stop:
        return f"*0\r\n"    
    if stop >= length:
        stop  = length - 1
    count = stop - start + 1
    res = f"*{count}\r\n"
    for idx in range(start, stop + 1):
        item = redis_list[key].get_elements(idx)
        res += f"${len(item)}\r\n{item}\r\n"
    return res

def lpop_func(args):
    key = args[0]
    # single‐element form: LPOP key
    if len(args) == 1:
        lst = redis_list.get(key)
        if not lst or lst.get_element_length() == 0:
            return "$-1\r\n"
        val = lst.pop_left()
        return f"${len(val)}\r\n{val}\r\n"
        remove = len(args[1])
    # multi‐element form: LPOP key count
    count = int(args[1])
    lst = redis_list.get(key)
    if not lst or lst.get_element_length() == 0:
        return "*0\r\n"
    # pop up to `count` items
    actual = min(count, lst.get_element_length())
    res = f"*{actual}\r\n"
    for _ in range(actual):
        val = lst.pop_left()
        res += f"${len(val)}\r\n{val}\r\n"
    return res

async def blpop_func(args):
    key = args[0]
    timeout = datetime.now().timestamp()+float(int(args[1]))/1000 if args[1] != 0 else 0
    lst = redis_list.setdefault(key, Redis_List(key))
    value = await lst.blpop(timeout)
    if value is None:
        # RESP nil array
        return "$-1\r\n"
    # return [ key, value ]
    return (
        f"*2\r\n"
        f"${len(key)}\r\n{key}\r\n"
        f"${len(value)}\r\n{value}\r\n"
    )


COMMANDS = {
    "ping":   ping_func,
    "echo":   echo_func,
    "set":    set_func,
    "get":    get_func,
    "rpush":  rpush_func,
    "lpush":  lpush_func,
    "lrange": lrange_func,
    "llen": llen_func,
    "lpop": lpop_func,
    "blpop": blpop_func,
}

def redis_command(cmd, args):
    print(cmd,args)
    fn = COMMANDS.get(cmd.lower())
    if not fn:
        return b"-ERR unknown command or invalid arguments\r\n"
    return fn(args)


if __name__ == "__main__":
    print(redis_command("set", ['apple', 'mango', 'px', '100']))
    print(redis_command("get",["apple"]))
            
        
    
