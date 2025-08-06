import socket
import asyncio
import argparse
import inspect
from app.convert_commands import convert_resp
from app.commands import redis_command

async def handle_client(reader, writer, server_state):
    client_state = {
        'multi_event': asyncio.Event(),
        'exec_event': [],
        'server_state': server_state,
        'is_replica': False,
        'handshake_step': 0  # 0: none, 1: PING, 2: REPLCONF port, 3: REPLCONF capa, 4: PSYNC
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
                try:
                    if server_state['role'] == 'slave' and cmd.lower() in ['set', 'incr', 'rpush', 'lpush', 'lpop', 'xadd']:
                        resp = "-ERR write commands not allowed on slave\r\n"
                    else:
                        if server_state['role'] == 'master':
                            if cmd.lower() == 'ping' and client_state['handshake_step'] == 0:
                                client_state['handshake_step'] = 1
                            elif cmd.lower() == 'replconf' and args and args[0].lower() == 'listening-port' and client_state['handshake_step'] == 1:
                                client_state['handshake_step'] = 2
                            elif cmd.lower() == 'replconf' and args and args[0].lower() == 'capa' and client_state['handshake_step'] == 2:
                                client_state['handshake_step'] = 3
                            elif cmd.lower() == 'psync' and args and len(args) == 2 and client_state['handshake_step'] == 3:
                                client_state['is_replica'] = True
                                server_state['replicas'].append(writer)
                                print(f"Added replica connection: {len(server_state['replicas'])} replicas connected")
                        
                        result = redis_command(cmd, args, client_state)
                        if inspect.iscoroutine(result):
                            result = await result
                        resp = result
                except Exception as e:
                    resp = f"-ERR server error: {str(e)}\r\n"
            
            if not client_state['is_replica'] or cmd.lower() in ['ping', 'replconf', 'psync']:
                writer.write(resp.encode() if isinstance(resp, str) else resp)
                await writer.drain()
    except Exception as e:
        print(f"Client error: {e}")
        writer.write(f"-ERR client error: {str(e)}\r\n".encode())
        await writer.drain()
    finally:
        if client_state['is_replica'] and writer in server_state['replicas']:
            server_state['replicas'].remove(writer)
            print(f"Removed replica connection: {len(server_state['replicas'])} replicas remaining")
        writer.close()
        await writer.wait_closed()

async def start_replication(master_host, master_port, server_state, replica_port):
    try:
        reader, writer = await asyncio.open_connection(master_host, master_port)
        print(f"Connected to master at {master_host}:{master_port}")
        
        # Step 1: Send PING
        writer.write(b"*1\r\n$4\r\nPING\r\n")
        await writer.drain()
        response = await reader.read(1024)
        print(f"Master response to PING: {response.decode('utf-8', errors='replace')}")
        if response != b"+PONG\r\n":
            raise Exception("PING failed: expected +PONG\r\n")

        # Step 2: Send REPLCONF listening-port <port>
        writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%d\r\n" % (
            len(str(replica_port)), replica_port
        ))
        await writer.drain()
        response = await reader.read(1024)
        print(f"Master response to REPLCONF listening-port: {response.decode('utf-8', errors='replace')}")
        if response != b"+OK\r\n":
            raise Exception("REPLCONF listening-port failed: expected +OK\r\n")

        # Step 3: Send REPLCONF capa psync2
        writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        await writer.drain()
        response = await reader.read(1024)
        print(f"Master response to REPLCONF capa: {response.decode('utf-8', errors='replace')}")
        if response != b"+OK\r\n":
            raise Exception("REPLCONF capa failed: expected +OK\r\n")

        # Step 4: Send PSYNC ? -1
        writer.write(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        await writer.drain()
        
        # Step 5: Read FULLRESYNC response
        buffer = b""
        while True:
            data = await reader.read(1024)
            if not data:
                raise Exception("Connection closed while reading FULLRESYNC")
            buffer += data
            print(f"FULLRESYNC buffer: len={len(buffer)}, content={buffer[:100]}")
            if b"\r\n" in buffer:
                fullresync_end = buffer.index(b"\r\n")
                response_str = buffer[:fullresync_end].decode('utf-8', errors='replace')
                print(f"Master response to PSYNC: {response_str}")
                if not response_str.startswith("+FULLRESYNC"):
                    raise Exception("PSYNC failed: expected +FULLRESYNC")
                parts = response_str.split()
                if len(parts) < 3 or parts[0] != "+FULLRESYNC":
                    raise Exception("Invalid FULLRESYNC response")
                master_replid = parts[1]
                master_offset = int(parts[2])
                server_state['master_replid'] = master_replid
                server_state['master_repl_offset'] = master_offset
                print(f"Received FULLRESYNC: replid={master_replid}, offset={master_offset}")
                buffer = buffer[fullresync_end + 2:]  # Clear FULLRESYNC response
                print(f"Buffer after FULLRESYNC: len={len(buffer)}, content={buffer[:100]}")
                break

        # Step 6: Read empty RDB file
        while True:
            if b"$" in buffer:
                rdb_start = buffer.index(b"$")
                end_of_length = buffer.index(b"\r\n", rdb_start)
                length_str = buffer[rdb_start + 1:end_of_length].decode('ascii')
                print(f"RDB length indicator: {length_str}")
                try:
                    length = int(length_str)
                except ValueError:
                    raise Exception(f"Invalid RDB length: {length_str}")
                rdb_data_start = end_of_length + 2
                print(f"RDB data start: {rdb_data_start}, expected length: {length}")
                while len(buffer) < rdb_data_start + length:
                    data = await reader.read(1024)
                    if not data:
                        raise Exception("Incomplete RDB file received")
                    buffer += data
                    print(f"RDB buffer updated: len={len(buffer)}, content={buffer[:100]}")
                rdb_data = buffer[rdb_data_start:rdb_data_start + length]
                print(f"Received RDB file: len={len(rdb_data)}, content={rdb_data}")
                buffer = buffer[rdb_data_start + length:]  # Clear RDB data
                print(f"Buffer after RDB: len={len(buffer)}, content={buffer[:100]}")
                break
            else:
                data = await reader.read(1024)
                if not data:
                    raise Exception("Connection closed while reading RDB file")
                buffer += data
                print(f"RDB buffer received: len={len(buffer)}, content={buffer[:100]}")

        # Step 7: Process propagated commands silently
        client_state = {
            'server_state': server_state,
            'multi_event': asyncio.Event(),
            'exec_event': []
        }
        client_state['multi_event'].set()  # Propagated commands execute immediately
        while True:
            if buffer:
                print(f"Command buffer before parsing: len={len(buffer)}, content={buffer[:100]}")
                cmd, args, consumed = convert_resp(buffer)
                print(f"Parsed command: cmd={cmd}, args={args}, consumed={consumed}")
                if cmd:
                    try:
                        print(f"Processing propagated command: {cmd} {args}")
                        result = redis_command(cmd, args, client_state, is_replica=True)
                        if inspect.iscoroutine(result):
                            result = await result
                        if result and cmd.lower() == 'replconf' and args and args[0].lower() == 'getack':
                            writer.write(result.encode() if isinstance(result, str) else result)
                            await writer.drain()
                        server_state['master_repl_offset'] += consumed  # Update offset after processing
                        buffer = buffer[consumed:]  # Clear processed bytes
                        print(f"Buffer after command: len={len(buffer)}, content={buffer[:100]}")
                    except Exception as e:
                        print(f"Replication command error: {cmd} {args} - {str(e)}")
                else:
                    data = await reader.read(1024)
                    if not data:
                        break
                    buffer += data
                    print(f"New data received: len={len(data)}, content={data[:100]}")
            else:
                data = await reader.read(1024)
                if not data:
                    break
                buffer += data
                print(f"New data received: len={len(data)}, content={data[:100]}")
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