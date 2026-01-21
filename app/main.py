import argparse
import inspect
from commands import redis_command,write_commands
from persistence import load_rdb, load_from_aof, save_rdb
from data_type.redisKey import rkey
import asyncio
from convert_commands import convert_resp
from router import execute_command, handle_replication_handshake

async def handle_client(reader, writer, server_state):
    # Initialize client context
    client_state = {
        'server_state': server_state,
        'multi_event': asyncio.Event(), # For transactions
        'exec_event': [],               # For transactions
        'is_replica': False,            # Default role
        'handshake_step': 0             # For replica handshake
    }
    client_state['multi_event'].set() 

    while True:
        data = await reader.read(1024)
        if not data: break

        # 1. Parse Input
        cmd_key, args, _ = convert_resp(data)
        if not cmd_key: continue

        # 2. Check for Replication Handshake (Special logic before standard routing)
        # This handles PING, REPLCONF, PSYNC sequences for replicas
        response, stop_processing = await handle_replication_handshake(
            cmd_key, args, client_state, server_state
        )
        
        # Send handshake response if one exists
        if response:
            writer.write(response if isinstance(response, bytes) else response.encode())
            await writer.drain()

        # 3. If it wasn't a handshake command (or handshake continues), execute standard command
        if not stop_processing:
            response = await execute_command(cmd_key, args, client_state)
            if response:
                writer.write(response.encode())
                await writer.drain()
        
        
async def start_replication(master_host, master_port, server_state, local_port):
    try:
        reader, writer = await asyncio.open_connection(master_host, master_port)
        print(f"Connected to master at {master_host}:{master_port}")
        
        # Replication handshake
        writer.write(b"*1\r\n$4\r\nPING\r\n")
        await writer.drain()
        data = await reader.read(1024)
        print(f"Master response to PING: {data.decode('utf-8', errors='replace')}")
        
        writer.write(f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(local_port))}\r\n{local_port}\r\n".encode())
        await writer.drain()
        data = await reader.read(1024)
        print(f"Master response to REPLCONF listening-port: {data.decode('utf-8', errors='replace')}")
        
        writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        await writer.drain()
        data = await reader.read(1024)
        print(f"Master response to REPLCONF capa: {data.decode('utf-8', errors='replace')}")
        
        writer.write(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        await writer.drain()
        data = await reader.read(1024)
        while b"$" not in data or data.count(b'\r\n') < 2:
            additional_data = await reader.read(1024)
            if not additional_data:
                print("Error: Connection closed while reading PSYNC response")
                return
            data += additional_data
            print(f"Debug: Read additional data, total length now: {len(data)}")
        
        if b"FULLRESYNC" in data:
            fullresync_line = data.split(b'\r\n')[0].decode('utf-8')
            print(f"Master response to PSYNC: {fullresync_line}")
            parts = fullresync_line.split()
            replid = parts[1]
            offset = int(parts[2]) if len(parts) > 2 else 0
            print(f"Received FULLRESYNC: replid={replid}, offset={offset}")
            
            # Read RDB file
            lines = data.split(b'\r\n')
            print(f"Debug: Found {len(lines)} lines in PSYNC response")
            
            for i, line in enumerate(lines[:3]):  # Print first 3 lines for debugging
                print(f"Debug: Line {i}: {line}")
            if len(lines) > 1:
                rdb_length_line = lines[1]
                if rdb_length_line.startswith(b'$'):
                    rdb_length = int(rdb_length_line[1:].decode('utf-8'))
                    print(f"RDB length indicator: {rdb_length}")
                else:
                    print(f"Error: Expected RDB length line starting with $, got: {rdb_length_line}")
                    return
            else:
                print("Error: No RDB length line found in PSYNC response")
                return
            print(f"RDB length indicator: {rdb_length}")
            
            # Calculate how much RDB data we already have in the initial response
            header_size = len(fullresync_line.encode()) + 2 + len(rdb_length_line) + 2  # +2 for each \r\n
            rdb_data_received = data[header_size:]
            print(f"Debug: header_size={header_size}, rdb_data_received length={len(rdb_data_received)}")
            remaining_rdb = rdb_length - len(rdb_data_received)
            
            if remaining_rdb > 0:
                print(f"Debug: Reading additional {remaining_rdb} bytes of RDB data")
                additional_rdb = await reader.read(remaining_rdb)
                rdb_data = rdb_data_received + additional_rdb
            else:
                rdb_data = rdb_data_received[:rdb_length]            
            print(f"Received RDB file: expected_len={rdb_length}, actual_len={len(rdb_data)}")
            
            # Set up buffer for remaining data (commands after RDB)
            buffer = rdb_data_received[rdb_length:] if len(rdb_data_received) > rdb_length else b""
            print(f"Debug: Initial buffer length={len(buffer)}")
            # Initialize processed_offset after receiving RDB
            server_state['processed_offset'] = 0
                    
        while True:
            try:
                if not buffer:
                    data = await reader.read(1024)
                    if not data:
                        break
                    buffer += data
                    print(f"New data received: len={len(data)}")
                
                cmd, args, consumed = convert_resp(buffer)
                if cmd:
                    server_state['processed_offset'] = server_state.get('processed_offset', 0) + consumed
                    if cmd.lower() == 'replconf' and args and args[0].lower() == 'getack':
                        offset = server_state.get('processed_offset', 0) - consumed
                        ack_resp = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(offset))}\r\n{offset}\r\n"
                        print(f"Sending REPLCONF ACK: offset={offset}")
                        writer.write(ack_resp.encode())
                        await writer.drain()
                    else:
                        # Create proper client_state for replica commands
                        replica_client_state = {
                            'multi_event': asyncio.Event(),
                            'exec_event': [],
                            'server_state': server_state,
                            'is_replica': True,
                            'writer': writer
                        }
                        replica_client_state['multi_event'].set()
                        result = redis_command(cmd, args, replica_client_state, is_replica=True)                       
                        if inspect.iscoroutine(result):
                            await result
                    buffer = buffer[consumed:]
                else:
                    break

            except Exception as e:
                print(f"Replication command error: {cmd} {args} - {str(e)}")
    except Exception as e:
        print(f"Replication error: {str(e)}")
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"Disconnected from master {master_host}:{master_port}")
        
async def main():
    print("Logs from your program will appear here!")
    parser = argparse.ArgumentParser(description="Redis server with custom port and replication")
    parser.add_argument("--port", type=int, default=6379, help="Port to run the server on")
    parser.add_argument(
        "--replicaof",
        type=str,
        default=None,
        help="Master server as 'host port' (e.g., '127.0.0.1 6379') to act as a slave, omit for master"
    )
    args = parser.parse_args()
    port = args.port
    master_host = None
    master_port = None
    if args.replicaof:
        try:
            master_host, master_port = args.replicaof.split()
            master_port = int(master_port)
        except ValueError:
            print("Error: --replicaof must be in format 'host port' (e.g., '127.0.0.1 6379')")
            return
    
    server_state = {
        'role': 'slave' if master_host else 'master',
        'master_host': master_host,
        'master_port': master_port,
        'master_replid': '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb',
        'master_repl_offset': 0,
        'replicas': []
    }
    
    recovery_state = {
        'multi_event': asyncio.Event(),
        'exec_event': [],
        'server_state': server_state,
        'is_replica': True # Prevents cycles during recovery
    }
    recovery_state['multi_event'].set()
    #  1. First, load the RDB snapshot (Point-in-time)
    
    initial_data = load_rdb()
    rkey._data = initial_data
    
    # 2. Then, replay the AOF (Delta changes since last snapshot)
    await load_from_aof(recovery_state)
    print(f"Starting server on port {port}")
    if master_host and master_port:
        print(f"Configured as slave of {master_host}:{master_port}")
    else:
        print("Configured as master")
    
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, server_state),
        "localhost",
        port
    )
    
    if master_host and master_port:
        asyncio.create_task(start_replication(master_host, master_port, server_state, port))
    
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())