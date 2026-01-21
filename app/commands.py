from datetime import datetime
from app.data_type.redisList import check_if_lists, rpush, lpush, llen, lrange, lpop_n, blpop
from app.data_type.redisStream import check_if_stream, xadd, xrange, xread
import app.data_type.redisKey as rkey
from typing import List
import asyncio
from app.convert_commands import build_resp_array
from app.persistence import log_to_aof
from app.registry  import redis_cmd

write_commands = {'set', 'incr', 'rpush', 'lpush', 'lpop', 'xadd'}


# Key Functions
@redis_cmd(is_write=False)
def ping_func(args, _):
    return "+PONG\r\n"

@redis_cmd(is_write=False)
def echo_func(args, _):
    return f"${len(args[0])}\r\n{args[0]}\r\n"

@redis_cmd(is_write=True)
def set_func(args, client_state):
    print(f"inset {args}")
    try:
        rkey.rset(args, client_state)
        return "+OK\r\n"
    except Exception as e:
        return f"-ERR set failed: {str(e)}\r\n"
    
@redis_cmd(is_write=False)
def get_func(args, client_state):
    try:
        val = rkey.get(args)
        if val is None:
            return "$-1\r\n"
        s = str(val)
        return f"${len(s)}\r\n{s}\r\n"
    except Exception as e:
        return f"-ERR get failed: {str(e)}\r\n"

@redis_cmd(is_write=False)
def type_func(args, _):
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

@redis_cmd(is_write=True)
def incr_func(args, client_state):
    try:
        return rkey.incr(args[0], client_state)
    except Exception as e:
        return f"-ERR incr failed: {str(e)}\r\n"

# List Functions
@redis_cmd(is_write=True)
def rpush_func(args, client_state):
    try:
        cnt = rpush(args[0], *args[1:])
        return f":{cnt}\r\n"
    except Exception as e:
        return f"-ERR rpush failed: {str(e)}\r\n"
    
@redis_cmd(is_write=True)
def lpush_func(args, client_state):
    try:
        cnt = lpush(args[0], *args[1:])
        return f":{cnt}\r\n"
    except Exception as e:
        return f"-ERR lpush failed: {str(e)}\r\n"

@redis_cmd(is_write=False)
def llen_func(args, _):
    try:
        return f":{llen(args[0])}\r\n"
    except Exception as e:
        return f"-ERR llen failed: {str(e)}\r\n"

@redis_cmd(is_write=False)
def lrange_func(args, _):
    try:
        arr = lrange(args[0], int(args[1]), int(args[2]))
        res = f"*{len(arr)}\r\n"
        for e in arr:
            res += f"${len(e)}\r\n{e}\r\n"
        return res
    except Exception as e:
        return f"-ERR lrange failed: {str(e)}\r\n"

@redis_cmd(is_write=True)
def lpop_func(args, client_state):
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

@redis_cmd(is_write=True)
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
@redis_cmd(is_write=True)
def xadd_func(args, client_state):
    try:
        key = args[0]
        new_id = args[1]
        field = {args[i]: args[i+1] for i in range(2, len(args), 2)}
        new_id = xadd(key, new_id, field)
        if new_id == "Error code 01":
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
        elif new_id == "Error code 02":
            return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
        return f"${len(new_id)}\r\n{new_id}\r\n"
    except Exception as e:
        return f"-ERR xadd failed: {str(e)}\r\n"

@redis_cmd(is_write=False)
def xrange_func(args, _):
    try:
        key, start, end = args
        arr = xrange(key, start, end)
        return arr
    except Exception as e:
        return f"-ERR xrange failed: {str(e)}\r\n"

@redis_cmd(is_write=False)
async def xread_func(args, client_state):
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

# Replication Commands
@redis_cmd(is_write=False)
def replconf_getack_func(args, client_state):
    if args[0].lower() != 'getack' or args[1] != '*':
        return "-ERR invalid REPLCONF GETACK arguments\r\n"
    offset = client_state['server_state'].get('processed_offset', 0)
    resp = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(offset))}\r\n{offset}\r\n"
    print(f"replconf_getack_func: offset={offset}, resp={resp!r}")
    return resp

@redis_cmd(is_write=False)
async def wait_func(args, client_state):
    if len(args) != 2:
        return "-ERR invalid WAIT arguments\r\n"
    try:
        num_replicas = int(args[0])
        timeout_ms = int(args[1])
        if num_replicas < 0 or timeout_ms < 0:
            return "-ERR WAIT arguments must be non-negative\r\n"
    except ValueError:
        return "-ERR WAIT arguments must be integers\r\n"
    
    server_state = client_state['server_state']
    target_offset = server_state['master_repl_offset']
    print(f"wait_func: num_replicas={num_replicas}, timeout_ms={timeout_ms}, target_offset={target_offset}")
    
    # Send REPLCONF GETACK * to all replicas
    getack_cmd = build_resp_array("REPLCONF", ["GETACK", "*"]).encode()
    for replica_writer, _ in server_state['replicas']:
        try:
            replica_writer.write(getack_cmd)
            await replica_writer.drain()
            print(f"wait_func: Sent REPLCONF GETACK * to replica {replica_writer}")
        except Exception as e:
            print(f"wait_func: Failed to send REPLCONF GETACK to replica: {e}")
    
    # Wait for acknowledgments or timeout
    start_time = asyncio.get_event_loop().time()
    timeout_s = timeout_ms / 1000.0
    while asyncio.get_event_loop().time() - start_time < timeout_s:
        synced_replicas = sum(1 for _, offset in server_state['replicas'] if offset >= target_offset)
        if synced_replicas >= num_replicas:
            print(f"wait_func: Returning {synced_replicas} replicas")
            return f":{synced_replicas}\r\n"
        await asyncio.sleep(0.01)
    
    synced_replicas = sum(1 for _, offset in server_state['replicas'] if offset >= target_offset)
    print(f"wait_func: Timeout reached, returning {synced_replicas} replicas")
    return f":{synced_replicas}\r\n"
# Command mapping

# app/commands.py

async def exec_func(args, client_state):
    if client_state['multi_event'].is_set():
        return "-ERR EXEC without MULTI\r\n"
    if not client_state['exec_event']:
        client_state['multi_event'].set()
        return "*0\r\n"

    replies = []
    # Commit transaction updates to the data store
    rkey.rkey.apply_transaction()
    client_state['multi_event'].set()

    # Import the centralized router logic (to avoid circular imports, import inside function)
    from router import execute_command 

    for c, a in client_state['exec_event']:
        # Re-route each queued command through the 3-layer system
        # This automatically handles AOF, Replication, and Sync/Async
        result = await execute_command(c.lower(), a, client_state)
        replies.append(result)

    client_state['exec_event'].clear()
    
    # Format the multi-bulk response
    out = f"*{len(replies)}\r\n"
    for r in replies:
        out += r if isinstance(r, str) else r.decode('utf-8', errors='replace')
    return out

@redis_cmd(is_write=False)
def multi_func(args, client_state):
    client_state['multi_event'].clear() # Set 'multi' mode
    return "+OK\r\n"

@redis_cmd(is_write=False)
def discard_func(args, client_state):
    if client_state['multi_event'].is_set():
        return "-ERR DISCARD without MULTI\r\n"
    client_state['exec_event'].clear()
    client_state['multi_event'].set()
    rkey.rkey.discard_transaction()
    return "+OK\r\n"

@redis_cmd(is_write=False)
def info_func(args, client_state):
    server_state = client_state['server_state']
    response = "# Replication\r\n"
    response += f"role:{server_state['role']}\r\n"
    if server_state['role'] == 'slave':
        response += f"master_host:{server_state['master_host']}\r\nmaster_port:{server_state['master_port']}\r\n"
    else:
        response += f"master_repl_offset:{server_state.get('master_repl_offset', 0)}\r\nmaster_replid:{server_state.get('master_replid', '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb')}\r\n"
    return f"${len(response)}\r\n{response}\r\n"

async def redis_command(cmd: str, args: List[str], client_state: dict, is_replica: bool = False) -> str:
    """
    Entry point for commands. In the new 3-layer architecture, 
    this delegates to the Router's execution engine.
    """
    cmd_key = cmd.lower()
    client_state['is_replica'] = is_replica # Ensure the state knows if it's a replication stream
    
    from router import execute_command
    return await execute_command(cmd_key, args, client_state)