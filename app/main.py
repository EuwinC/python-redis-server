import socket
import asyncio
import argparse
import inspect
from app.convert_commands import convert_resp,build_resp_array
from app.commands import redis_command,write_commands

async def handle_client(reader, writer, server_state):
    client_state = {
        'multi_event': asyncio.Event(),
        'exec_event': [],
        'server_state': server_state,
        'is_replica': False,
        'handshake_step': 0,
        'writer': writer
    }
    client_state['multi_event'].set()
    
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            cmd, args, consumed = convert_resp(data)
            if not cmd:
                resp = "-ERR invalid RESP format\r\n"
            else:
                if server_state['role'] == 'slave' and cmd.lower() in ['set', 'incr', 'rpush', 'lpush', 'lpop', 'xadd']:
                    resp = "-ERR write commands not allowed on slave\r\n"
                elif server_state['role'] == 'master':
                    if cmd.lower() == 'ping' and client_state['handshake_step'] == 0:
                        client_state['handshake_step'] = 1
                        resp = "+PONG\r\n"
                    elif cmd.lower() == 'replconf' and args and args[0].lower() == 'listening-port' and client_state['handshake_step'] == 1:
                        client_state['handshake_step'] = 2
                        resp = "+OK\r\n"
                    elif cmd.lower() == 'replconf' and args and args[0].lower() == 'capa' and client_state['handshake_step'] == 2:
                        client_state['handshake_step'] = 3
                        resp = "+OK\r\n"
                    elif cmd.lower() == 'psync' and args and len(args) == 2 and client_state['handshake_step'] == 3:
                        replid = server_state.get('master_replid', '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb')
                        offset = server_state.get('master_repl_offset', 0)
                        empty_rdb = bytes.fromhex(
                            '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040'
                            'fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'
                        )
                        writer.write(f"+FULLRESYNC {replid} {offset}\r\n${len(empty_rdb)}\r\n".encode() + empty_rdb)
                        await writer.drain()
                        server_state['replicas'].append((writer, 0))
                        print(f"Added replica connection: {len(server_state['replicas'])} replicas connected")
                        resp = None
                    elif cmd.lower() == 'replconf' and args and args[0].lower() == 'ack' and not client_state['is_replica']:
                        offset = int(args[1])
                        for i, (w, _) in enumerate(server_state['replicas']):
                            if w == writer:
                                server_state['replicas'][i] = (w, offset)
                                print(f"Updated replica offset to {offset}")
                        resp = None
                    else:
                        # Check if command is async
                        result = redis_command(cmd, args, client_state, client_state['is_replica'])
                        if inspect.iscoroutine(result):
                            resp = await result
                        else:
                            resp = result
                        print(f"handle_client: cmd={cmd}, resp={resp!r}, type={type(resp)}")
                elif client_state['is_replica'] and cmd.lower() == 'replconf' and args and args[0].lower() == 'getack':
                    offset = server_state.get('processed_offset', 0)
                    ack_cmd = build_resp_array("REPLCONF", ["ACK", str(offset)])
                    writer.write(ack_cmd.encode())
                    await writer.drain()
                    resp = None
                else:
                    # Check if command is async
                    result = redis_command(cmd, args, client_state, client_state['is_replica'])
                    if inspect.iscoroutine(result):
                        resp = await result
                    else:
                        resp = result
                    print(f"handle_client: cmd={cmd}, resp={resp!r}, type={type(resp)}")
                
                if resp and (not client_state['is_replica'] or cmd.lower() in ['ping', 'replconf', 'psync']):
                    print(f"handle_client: Writing response, cmd={cmd}, resp={resp!r}")
                    writer.write(resp.encode() if isinstance(resp, str) else resp)
                    await writer.drain()
                    print(f"handle_client: Response sent for cmd={cmd}")
    finally:
        if client_state['is_replica']:
            server_state['replicas'] = [(w, o) for w, o in server_state['replicas'] if w != writer]
        writer.close()
        await writer.wait_closed()
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