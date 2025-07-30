from datetime import datetime
import asyncio
from app.data_type.redisList import check_if_lists, rpush, lpush, llen, lrange, lpop_n, blpop
from app.data_type.redisStream import check_if_stream, xadd
from app.data_type.kvstore import store
def ping_func(args):
    return "+PONG\r\n"

def echo_func(args):
    return  f"${len(args[0])}\r\n{args[0]}\r\n"

def set_func(args):
    key, val = args[0], args[1]
    ttl = float(args[3]) / 1000 if len(args) == 4 else None
    store.set_string(key, val, ttl)
    return "+OK\r\n"


def get_func(args):
    val = store.get_string(args[0])
    return f"${len(val)}\r\n{val}\r\n" if val is not None else "$-1\r\n"


def type_func(args):
    key = args[0]
    t = store.get_type(key)
    if t == "string":
        return "+string\r\n"
    if check_if_lists(key):
        return "+lists\r\n"
    if check_if_stream(key):
        return "+stream\r\n"
    return "+none\r\n"

#List functions
def rpush_func(args):
    cnt = rpush(args[0], *args[1:])
    return f":{cnt}\r\n"

def lpush_func(args):
    cnt = lpush(args[0], *args[1:])
    return f":{cnt}\r\n"

def llen_func(args):
    return f":{llen(args[0])}\r\n"

def lrange_func(args):
    arr = lrange(args[0], int(args[1]), int(args[2]))
    res = f"*{len(arr)}\r\n"
    for e in arr:
        res += f"${len(e)}\r\n{e}\r\n"
    return res

def lpop_func(args):
    if len(args) == 1:
        arr = lpop_n(args[0], 1)
        return f"${len(arr[0])}\r\n{arr[0]}\r\n" if arr else "$-1\r\n"
    else:
        arr = lpop_n(args[0], int(args[1]))
        res = f"*{len(arr)}\r\n"
        for e in arr:
            res += f"${len(e)}\r\n{e}\r\n"
        return res

async def blpop_func(args):
    tup = await blpop(args[0], float(args[1]) if len(args)>1 else 0)
    if not tup:
        return "$-1\r\n"
    key, val = tup
    return f"*2\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"

#Stream functions
def xadd_func(args):
    key = args[0]
    new_id = args[1]
    field = dict() #field_name:field_val
    pairs = args[2:]
    field = {pairs[i]: pairs[i+1] for i in range(0, len(pairs), 2)}
    new_id = xadd(key,new_id,field) 
    return f"${len(new_id)}\r\n{new_id}\r\n"

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
    "type": type_func,
    "xadd": xadd_func,
}

def redis_command(cmd, args):
    print(cmd,args)
    fn = COMMANDS.get(cmd.lower())
    if not fn:
        return b"-ERR unknown command or invalid arguments\r\n"
    return fn(args)


if __name__ == "__main__":
    print(redis_command("xadd", ['mango', '0-1', 'foo', 'bar']))
    print(redis_command("get",["apple"]))
            
        
    
