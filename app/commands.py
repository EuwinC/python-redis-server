from datetime import datetime
from app.data_type.redisList import check_if_lists, rpush, lpush, llen, lrange, lpop_n, blpop
from app.data_type.redisStream import check_if_stream, xadd, xrange, xread
import app.data_type.redisKey as rkey
from typing import List
import asyncio
from app.convert_commands import build_resp_array

write_commands = {'set', 'incr', 'rpush', 'lpush', 'lpop', 'xadd'}

# Key Functions
def ping_func(args, _):
    return "+PONG\r\n"

def echo_func(args, _):
    return f"${len(args[0])}\r\n{args[0]}\r\n"

def set_func(args, client_state):
    print(f"inset {args}")
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

def llen_func(args, _):
    try:
        return f":{llen(args[0])}\r\n"
    except Exception as e:
        return f"-ERR llen failed: {str(e)}\r\n"

def lrange_func(args, _):
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
        field = {args[i]: args[i+1] for i in range(2, len(args), 2)}
        new_id = xadd(key, new_id, field)
        if new_id == "Error code 01":
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n"
        elif new_id == "Error code 02":
            return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
        return f"${len(new_id)}\r\n{new_id}\r\n"
    except Exception as e:
        return f"-ERR xadd failed: {str(e)}\r\n"

def xrange_func(args, _):
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

# Replication Commands
def replconf_getack_func(args, client_state):
    if args[0].lower() != 'getack' or args[1] != '*':
        return "-ERR invalid REPLCONF GETACK arguments\r\n"
    offset = client_state['server_state'].get('processed_offset', 0)
    resp = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(offset))}\r\n{offset}\r\n"
    print(f"replconf_getack_func: offset={offset}, resp={resp!r}")
    return resp

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
COMMANDS = {
    'ping': ping_func,
    'echo': echo_func,
    'set': set_func,
    'get': get_func,
    'type': type_func,
    'incr': incr_func,
    'rpush': rpush_func,
    'lpush': lpush_func,
    'llen': llen_func,
    'lrange': lrange_func,
    'lpop': lpop_func,
    'blpop': blpop_func,
    'xadd': xadd_func,
    'xrange': xrange_func,
    'xread': xread_func,
    'replconf': lambda args, client_state: "+OK\r\n" if args[0].lower() in ['listening-port', 'capa'] else replconf_getack_func(args, client_state),
    'wait': wait_func,
    'multi': lambda args, client_state: (
        "-ERR MULTI calls can not be nested\r\n" if not client_state['multi_event'].is_set() else (
            client_state['multi_event'].clear() or "+OK\r\n"
        )
    ),
    'discard': lambda args, client_state: (
        "-ERR DISCARD without MULTI\r\n" if client_state['multi_event'].is_set() else (
            client_state['multi_event'].set() or
            client_state['exec_event'].clear() or
            rkey.rkey.discard_transaction() or
            "+OK\r\n"
        )
    ),
    'exec': lambda args, client_state: exec_func(args, client_state)
}

async def exec_func(args, client_state):
    if client_state['multi_event'].is_set():
        return "-ERR EXEC without MULTI\r\n"
    if not client_state['exec_event']:
        client_state['multi_event'].set()
        return "*0\r\n"
    server_state = client_state['server_state']
    replies = []
    try:
        rkey.rkey.apply_transaction()
        client_state['multi_event'].set()
        for c, a in client_state['exec_event']:
            fn = COMMANDS.get(c.lower())
            if not fn:
                result = "-ERR unknown command or invalid arguments\r\n"
            elif c.lower() in ("xread", "blpop", "wait"):
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
            if server_state['role'] == 'master' and c.lower() in write_commands:
                cmd_data = build_resp_array(c, a)
                cmd_data_bytes = cmd_data.encode()
                server_state['master_repl_offset'] += len(cmd_data_bytes)
                for replica_writer, _ in server_state['replicas']:
                    try:
                        replica_writer.write(cmd_data_bytes)
                        await replica_writer.drain()
                        print(f"Propagated {c} to replica {replica_writer}")
                    except Exception as e:
                        print(f"Failed to propagate {c} to replica: {e}")
        client_state['exec_event'].clear()
        out = f"*{len(replies)}\r\n"
        for r in replies:
            out += r if isinstance(r, str) else r.decode('utf-8', errors='replace')
        return out
    except Exception as e:
        client_state['multi_event'].set()
        client_state['exec_event'].clear()
        return f"-ERR exec failed: {str(e)}\r\n"

def replication_process(key: str, args: List[str], client_state: dict, server_state: dict) -> str:
    if key == "info":
        response = "# Replication\r\n"
        response += f"role:{server_state['role']}\r\n"
        if server_state['role'] == 'slave':
            response += f"master_host:{server_state['master_host']}\r\nmaster_port:{server_state['master_port']}\r\n"
        else:
            response += f"master_repl_offset:{server_state.get('master_repl_offset', 0)}\r\nmaster_replid:{server_state.get('master_replid', '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb')}\r\n"
        return f"${len(response)}\r\n{response}\r\n"

    if key == "replconf" and args:
        if args[0].lower() in ['listening-port', 'capa']:
            return COMMANDS['replconf'](args, client_state)
        elif args[0].lower() == 'getack':
            return COMMANDS['replconf'](args, client_state)
        elif args[0].lower() == 'ack':
           # Handle REPLCONF ACK <offset> from replica
            try:
                offset = int(args[1])
                # Find and update the replica's offset
                writer = client_state.get('writer')  # We'll pass this from main
                for i, (replica_writer, _) in enumerate(server_state['replicas']):
                    if replica_writer == writer:
                        server_state['replicas'][i] = (replica_writer, offset)
                        print(f"Updated replica offset to {offset}")
                        break
                return "+OK\r\n"
            except (ValueError, IndexError):
                return "-ERR invalid ACK format\r\n"
            
    if key == "psync" and len(args) == 2:
        if server_state['role'] == 'master':
            replid = server_state.get('master_replid', '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb')
            offset = server_state.get('master_repl_offset', 0)
            empty_rdb = bytes.fromhex(
                '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040'
                'fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'
            )
            return f"+FULLRESYNC {replid} {offset}\r\n${len(empty_rdb)}\r\n".encode() + empty_rdb
        return "-ERR PSYNC only allowed on master\r\n"
    
    return "-ERR invalid replication command\r\n"

def transaction_func(key: str, args: List[str], client_state: dict, server_state: dict) -> str:
    return COMMANDS[key](args, client_state)

async def redis_command(cmd: str, args: List[str], client_state: dict, is_replica: bool = False) -> str:
    key = cmd.lower()
    server_state = client_state.get('server_state', {'role': 'master'})
    print(f"redis_command: start, cmd={key}, args={args}, is_replica={is_replica}")
    
    try:
        if key == "wait":
            resp = await wait_func(args, client_state)
            print(f"redis_command: cmd={key}, resp={resp!r}, type={type(resp)}")
            return resp
        
        replication_key = {'info', 'replconf', 'psync'}
        if key in replication_key:
            res = replication_process(key, args, client_state, server_state)
            print(f"redis_command: cmd={key}, resp={res!r}, type={type(res)}")
            return res
        
        transaction_key = {"multi", "discard", "exec"}
        if key in transaction_key:
            if key == "exec":
                res = await exec_func(args, client_state)
            else:
                res = transaction_func(key, args, client_state, server_state)
            print(f"redis_command: cmd={key}, resp={res!r}, type={type(res)}")
            return res

        # Queue non-transaction commands during MULTI
        if not client_state['multi_event'].is_set() and key != "exec":
            client_state['exec_event'].append([cmd, args])
            print(f"redis_command: Queued cmd={key}, args={args}")
            return "+QUEUED\r\n"

        # Write commands to propagate
        if key in write_commands:
            if not client_state['multi_event'].is_set():
                return "+QUEUED\r\n"
            fn = COMMANDS[key]
            resp = await fn(args, client_state) if key in ("xread", "blpop") else fn(args, client_state)
            if server_state['role'] == 'master' and not is_replica and client_state['multi_event'].is_set():
                cmd_data = build_resp_array(cmd, args)
                cmd_data_bytes = cmd_data.encode()
                server_state['master_repl_offset'] += len(cmd_data_bytes)
                for replica_writer, _ in server_state['replicas']:
                    replica_writer.write(cmd_data_bytes)
                    await replica_writer.drain()
            if is_replica:
                server_state['processed_offset'] = server_state.get('processed_offset', 0) + len(cmd_data_bytes)
                return None
            return resp

        # Non-write commands
        fn = COMMANDS.get(key)
        print(f"redis_command: non-write command, key={key}, fn={fn}")
        if not fn:
            return "-ERR unknown command or invalid arguments\r\n"
        resp = await fn(args, client_state) if key in ("xread", "blpop") else fn(args, client_state)
        print(f"redis_command: cmd={key}, resp={resp!r}, type={type(resp)}")
        return resp
    
    except Exception as e:
        print(f"redis_command: General error, cmd={key}, args={args}, error={str(e)}")
        return f"-ERR server error: {str(e)}\r\n"