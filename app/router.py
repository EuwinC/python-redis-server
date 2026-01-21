empty_rdb = bytes.fromhex(
    '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040'
    'fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'
)
from registry import COMMAND_REGISTRY,WRITE_COMMANDS
from persistence import log_to_aof
from convert_commands import build_resp_array
import inspect

def check_permissions(server_role, cmd_key):
    """Returns an error message if the command is blocked, else None."""
    if server_role == 'slave' and cmd_key in WRITE_COMMANDS:
        return "-ERR write commands not allowed on slave\r\n"
    return None

async def handle_replication_handshake(cmd, args, client_state, server_state):
    """
    Handles the master-side logic for a replica connecting.
    Returns (response_to_send, should_continue_to_standard_commands)
    """
    step = client_state.get('handshake_step', 0)
    
    match (cmd.lower(), step):
        case ('ping', 0):
            client_state['handshake_step'] = 1
            return "+PONG\r\n", False
            
        case ('replconf', 1) if args and args[0].lower() == 'listening-port':
            client_state['handshake_step'] = 2
            return "+OK\r\n", False
            
        case ('replconf', 2) if args and args[0].lower() == 'capa':
            client_state['handshake_step'] = 3
            return "+OK\r\n", False
            
        case ('psync', 3) if len(args) == 2:
            # Prepare the FULLRESYNC sequence
            replid = server_state.get('master_replid', '8371...')
            offset = server_state.get('master_repl_offset', 0)
            # You can return the bytes directly to the networking layer
            resync_header = f"+FULLRESYNC {replid} {offset}\r\n".encode()
            # ... add RDB data ...
            return (resync_header + empty_rdb), False
            
    return None, True # Not a handshake command, proceed to normal execution


async def handle_persistence_and_replication(cmd, args, client_state):
    """
    Post-execution middleware to handle AOF logging and Master-Slave propagation.
    """
    server_state = client_state['server_state']
    
    # 1. Convert the command back to RESP format
    # This is what will be stored in the AOF and sent to replicas
    cmd_resp = build_resp_array(cmd, args)
    
    # 2. AOF Persistence
    # Only the master logs to AOF to avoid duplicate write logs in the cluster
    if server_state['role'] == 'master':
        log_to_aof(cmd_resp)
    
    # 3. Replication Propagation
    if server_state['role'] == 'master':
        cmd_bytes = cmd_resp.encode()
        
        # Update the master offset globally so WAIT command can track it
        server_state['master_repl_offset'] += len(cmd_bytes)
        
        # Send the command to every connected replica
        for writer, _ in server_state['replicas']:
            try:
                writer.write(cmd_bytes)
                await writer.drain() # Ensure the replica actually receives the write
            except Exception as e:
                print(f"Failed to propagate to replica: {e}")

# router.py snippet

async def execute_command(cmd_key, args, client_state):
    fn = COMMAND_REGISTRY.get(cmd_key)
    if not fn:
        return "-ERR unknown command\r\n"

    # TRANSACTION GUARD: If multi_event is NOT set, we are in a transaction
    if not client_state['multi_event'].is_set() and cmd_key not in ('exec', 'discard', 'multi'):
        client_state['exec_event'].append((cmd_key, args))
        return "+QUEUED\r\n"

    # Standard Execution (Layer 3)
    result = await fn(args, client_state) if inspect.iscoroutinefunction(fn) else fn(args, client_state)

    # Post-Execution (AOF & Replication)
    if getattr(fn, 'is_write', False) and not client_state['is_replica']:
        await handle_persistence_and_replication(cmd_key, args, client_state)

    return result