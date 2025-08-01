from datetime import datetime
from app.data_type.redisList import check_if_lists, rpush, lpush, llen, lrange, lpop_n, blpop
from app.data_type.redisStream import check_if_stream, xadd, xrange, xread
import app.data_type.redisKey as rkey
from typing import List

# Key Functions
def ping_func(args,_):
    return "+PONG\r\n"

def echo_func(args,_):
    return f"${len(args[0])}\r\n{args[0]}\r\n"

def set_func(args, client_state):
    if not client_state['multi_event'].is_set():
        return "+QUEUED\r\n"
    try:
        rkey.rset(args, client_state)
        return "+OK\r\n"
    except Exception as e:
        return f"-ERR set failed: {str(e)}\r\n"

def get_func(args, client_state):
    try:
        val = rkey.get(args)
        if val is None:
            return "$-1\r\n"
        s = str(val)
        return f"${len(s)}\r\n{s}\r\n"
    except Exception as e:
        return f"-ERR get failed: {str(e)}\r\n"

def type_func(args,_):
    try:
        key = args[0]
        t = rkey.check_type(key)
        if t is not None:
            return f"+{t}\r\n"
        if check_if_lists(key):
            return "+lists\r\n"
        if check_if_stream(key):
            return "+stream\r\n"
        return "+none\r\n"
    except Exception as e:
        return f"-ERR type failed: {str(e)}\r\n"

def incr_func(args, client_state):
    if not client_state['multi_event'].is_set():
        return "+QUEUED\r\n"
    try:
        return rkey.incr(args[0], client_state)
    except Exception as e:
        return f"-ERR incr failed: {str(e)}\r\n"

# List Functions
def rpush_func(args, client_state):
    if not client_state['multi_event'].is_set():
        return "+QUEUED\r\n"
    try:
        cnt = rpush(args[0], *args[1:])
        return f":{cnt}\r\n"
    except Exception as e:
        return f"-ERR rpush failed: {str(e)}\r\n"

def lpush_func(args, client_state):
    if not client_state['multi_event'].is_set():
        return "+QUEUED\r\n"
    try:
        cnt = lpush(args[0], *args[1:])
        return f":{cnt}\r\n"
    except Exception as e:
        return f"-ERR lpush failed: {str(e)}\r\n"

def llen_func(args,_):
    try:
        return f":{llen(args[0])}\r\n"
    except Exception as e:
        return f"-ERR llen failed: {str(e)}\r\n"

def lrange_func(args,_):
    try:
        arr = lrange(args[0], int(args[1]), int(args[2]))
        res = f"*{len(arr)}\r\n"
        for e in arr:
            res += f"${len(e)}\r\n{e}\r\n"
        return res
    except Exception as e:
        return f"-ERR lrange failed: {str(e)}\r\n"

def lpop_func(args, client_state):
    if not client_state['multi_event'].is_set():
        return "+QUEUED\r\n"
    try:
        if len(args) == 1:
            arr = lpop_n(args[0], 1)
            return f"${len(arr[0])}\r\n{arr[0]}\r\n" if arr else "$-1\r\n"
        else:
            arr = lpop_n(args[0], int(args[1]))
            res = f"*{len(arr)}\r\n"
            for e in arr:
                res += f"${len(e)}\r\n{e}\r\n"
            return res
    except Exception as e:
        return f"-ERR lpop failed: {str(e)}\r\n"

async def blpop_func(args, _):
    try:
        tup = await blpop(args[0], float(args[1]) if len(args) > 1 else 0)
        if not tup:
            return "$-1\r\n"
        key, val = tup
        return f"*2\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
    except Exception as e:
        return f"-ERR blpop failed: {str(e)}\r\n"

# Stream Functions
def xadd_func(args, client_state):
    if not client_state['multi_event'].is_set():
        return "+QUEUED\r\n"
    try:
        key = args[0]
        new_id = args[1]
        field = dict()
        pairs = args[2:]
        field = {pairs[i]: pairs[i+1] for i in range(0, len(pairs), 2)}
        new_id = xadd(key, new_id, field)
        if new_id == "Error code 01":
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
        elif new_id == "Error code 02":
            return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
        return f"${len(new_id)}\r\n{new_id}\r\n"
    except Exception as e:
        return f"-ERR xadd failed: {str(e)}\r\n"

def xrange_func(args, _):
    print("hi",args)
    try:
        key, start, end = args
        arr = xrange(key, start, end)
        return arr
    except Exception as e:
        return f"-ERR xrange failed: {str(e)}\r\n"

async def xread_func(args, client_state):
    if not client_state['multi_event'].is_set():
        return "+QUEUED\r\n"
    try:
        block_ms = None
        stream_idx = 0
        if args[0].lower() == "block":
            if len(args) < 4 or args[2].lower() != "streams":
                return "-ERR syntax error\r\n"
            try:
                block_ms = int(args[1])
                if block_ms < 0:
                    return "-ERR invalid block timeout\r\n"
            except ValueError:
                return "-ERR block timeout is not an integer\r\n"
            stream_idx = 2
        if args[stream_idx].lower() != "streams":
            return "-ERR syntax error: expected STREAMS\r\n"
        stream_idx += 1
        if len(args) < stream_idx + 2:
            return "-ERR wrong number of arguments for 'xread' command\r\n"
        mid = (len(args) - stream_idx) // 2
        keys = args[stream_idx:stream_idx + mid]
        data_ids = args[stream_idx + mid:]
        if len(keys) != len(data_ids):
            return "-ERR number of keys does not match number of IDs\r\n"
        return await xread(keys, data_ids, block_ms)
    except Exception as e:
        return f"-ERR xread failed: {str(e)}\r\n"

async def multi_func(args, client_state):
    try:
        if client_state['multi_event'].is_set():
            client_state['multi_event'].clear()
            return "+OK\r\n"
        else:
            return "-ERR MULTI calls can not be nested\r\n"
    except Exception as e:
        return f"-ERR multi failed: {str(e)}\r\n"

COMMANDS = {
    "ping": ping_func,
    "echo": echo_func,
    "set": set_func,
    "get": get_func,
    "rpush": rpush_func,
    "lpush": lpush_func,
    "lrange": lrange_func,
    "llen": llen_func,
    "lpop": lpop_func,
    "blpop": blpop_func,
    "type": type_func,
    "xadd": xadd_func,
    "xrange": xrange_func,
    "xread": xread_func,
    "incr": incr_func,
    "multi": multi_func,
}

async def redis_command(cmd: str, args: List[str], client_state) -> str:
    key = cmd.lower()
    print(f"Command: {key}, Args: {args}, Exec Queue: {client_state['exec_event']}, Multi: {client_state['multi_event'].is_set()}")
    if key == "multi":
        return await multi_func(args, client_state)
    if key == "discard":
        if client_state['multi_event'].is_set():
            return "-ERR DISCARD without MULTI\r\n"
        else:
            client_state['multi_event'].set() 
            client_state['exec_event'].clear()
            rkey.rkey.discard_transaction()    
        return "+OK\r\n"
    if key == "exec":
        if client_state['multi_event'].is_set():
            return "-ERR EXEC without MULTI\r\n"
        if not client_state['exec_event']:
            client_state['multi_event'].set()
            return "*0\r\n"
        replies = []
        try:
            # Apply transaction updates to rkey._data
            rkey.rkey.apply_transaction()
            client_state['multi_event'].set()  # Move set after apply_transaction
            for c, a in client_state['exec_event']:
                fn = COMMANDS.get(c.lower())
                if not fn:
                    result = "-ERR unknown command or invalid arguments\r\n"
                elif c.lower() in ("xread", "blpop", "multi"):
                    try:
                        result = await fn(a, client_state)
                    except Exception as e:
                        result = f"-ERR {c} failed: {str(e)}\r\n"
                else:
                    try:
                        result = fn(a, client_state)
                    except Exception as e:
                        result = f"-ERR {c} failed: {str(e)}\r\n"
                replies.append(result)
                print(f"EXEC: {c} {a} -> {result!r}")
            client_state['exec_event'].clear()
            out = f"*{len(replies)}\r\n"
            for r in replies:
                out += r
            return out
        except Exception as e:
            client_state['multi_event'].set()
            client_state['exec_event'].clear()
            return f"-ERR exec failed: {str(e)}\r\n"
    if not client_state['multi_event'].is_set() and key != "exec":
        client_state['exec_event'].append([cmd, args])
        return "+QUEUED\r\n"

    fn = COMMANDS.get(key)
    if not fn:
        return "-ERR unknown command or invalid arguments\r\n"
    if key in ("xread", "blpop", "multi"):
        return await fn(args, client_state)
    return fn(args, client_state)