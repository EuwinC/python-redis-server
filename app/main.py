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
        'is_replica': False,  # Track if client is a replica
        'handshake_step': 0  # 0: none, 1: PING, 2: REPLCONF port, 3: REPLCONF capa, 4: PSYNC
    }
    client_state['multi_event'].set()
    
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            cmd, args = convert_resp(data)
            if not cmd:
                resp = "-ERR invalid RESP format\r\n"
            else:
                try:
                    # Block write commands on slave
                    if server_state['role'] == 'slave' and cmd.lower() in ['set', 'incr', 'rpush', 'lpush', 'lpop', 'xadd']:
                        resp = "-ERR write commands not allowed on slave\r\n"
                    else:
                        # Track handshake for replica detection
                        if server_state['role'] == 'master':
                            if cmd.lower() == 'ping' and client_state['handshake_step'] == 0:
                                client_state['handshake_step'] = 1
                            elif cmd.lower() == 'replconf' and args[0].lower() == 'listening-port' and client_state['handshake_step'] == 1:
                                client_state['handshake_step'] = 2
                            elif cmd.lower() == 'replconf' and args[0].lower() == 'capa' and client_state['handshake_step'] == 2:
                                client_state['handshake_step'] = 3
                            elif cmd.lower() == 'psync' and client_state['handshake_step'] == 3:
                                client_state['is_replica'] = True
                                server_state['replicas'].append(writer)
                                print(f"Added replica connection: {len(server_state['replicas'])} replicas connected")
                        
                        result = redis_command(cmd, args, client_state)
                        if inspect.iscoroutine(result):
                            result = await result
                        resp = result
                except Exception as e:
                    resp = f"-ERR server error: {str(e)}\r\n"
            
            # Send response unless client is a confirmed replica processing non-handshake commands
            if not (client_state['is_replica'] and cmd.lower() not in ['ping', 'replconf', 'psync']):
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
        print(f"Master response to PING: {response.decode()}")
        if response != b"+PONG\r\n":
            raise Exception("PING failed: expected +PONG\r\n")

        # Step 2: Send REPLCONF listening-port <port>
        writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%d\r\n" % (
            len(str(replica_port)), replica_port
        ))
        await writer.drain()
        response = await reader.read(1024)
        print(f"Master response to REPLCONF listening-port: {response.decode()}")
        if response != b"+OK\r\n":
            raise Exception("REPLCONF listening-port failed: expected +OK\r\n")

        # Step 3: Send REPLCONF capa psync2
        writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        await writer.drain()
        response = await reader.read(1024)
        print(f"Master response to REPLCONF capa: {response.decode()}")
        if response != b"+OK\r\n":
            raise Exception("REPLCONF capa failed: expected +OK\r\n")

        # Step 4: Send PSYNC ? -1
        writer.write(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        await writer.drain()
        response = await reader.read(1024)
        print(f"Master response to PSYNC: {response.decode()}")
        if not response.startswith(b"+FULLRESYNC"):
            raise Exception("PSYNC failed: expected +FULLRESYNC")
        
        # Parse FULLRESYNC response
        response_str = response.decode()
        parts = response_str.split()
        if len(parts) < 3 or parts[0] != "+FULLRESYNC":
            raise Exception("Invalid FULLRESYNC response")
        master_replid = parts[1]
        master_offset = int(parts[2].strip("\r\n"))
        server_state['master_replid'] = master_replid
        server_state['master_repl_offset'] = master_offset
        print(f"Received FULLRESYNC: replid={master_replid}, offset={master_offset}")

        # Step 5: Read empty RDB file
        rdb_response = await reader.read(1024)
        if not rdb_response.startswith(b"$"):
            raise Exception("Expected RDB file with $<length>\\r\\n")
        end_of_length = rdb_response.index(b"\r\n")
        length = int(rdb_response[1:end_of_length].decode())
        print(f"Expected RDB file length: {length}")
        rdb_data = rdb_response[end_of_length + 2:end_of_length + 2 + length]
        if len(rdb_data) != length:
            while len(rdb_data) < length:
                more_data = await reader.read(length - len(rdb_data))
                if not more_data:
                    raise Exception("Incomplete RDB file received")
                rdb_data += more_data
        print(f"Received empty RDB file ({len(rdb_data)} bytes)")

        # Step 6: Process propagated commands silently
        while True:
            data = await reader.read(1024)
            if not data:
                break
            cmd, args = convert_resp(data)
            if cmd:
                try:
                    result = redis_command(cmd, args, {'server_state': server_state}, is_replica=True)
                    if inspect.iscoroutine(result):
                        await result
                except Exception as e:
                    print(f"Replication command error: {e}")
    except Exception as e:
        print(f"Replication error: {e}")
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
        'replicas': []  # Track confirmed replica connections
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