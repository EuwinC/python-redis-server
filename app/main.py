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
        'server_state': server_state
    }
    client_state['multi_event'].set()  # Initially not in transaction
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
                    # Reject write commands if server is a slave
                    if server_state['role'] == 'slave' and cmd.lower() in ['set', 'del', 'incr', 'decr']:
                        resp = "-ERR write commands not allowed on slave\r\n"
                    else:
                        result = redis_command(cmd, args, client_state)
                        if inspect.iscoroutine(result):
                            result = await result
                        resp = result
                except Exception as e:
                    resp = f"-ERR server error: {str(e)}\r\n"
            writer.write(resp.encode() if isinstance(resp, str) else resp)
            await writer.drain()
    except Exception as e:
        print(f"Client error: {e}")
        writer.write(f"-ERR client error: {str(e)}\r\n".encode())
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

async def start_replication(master_host, master_port, server_state):
    try:
        reader, writer = await asyncio.open_connection(master_host, master_port)
        print(f"Connected to master at {master_host}:{master_port}")
        
        # Send PING to verify connection
        writer.write(b"*1\r\n$4\r\nPING\r\n")
        await writer.drain()
        response = await reader.read(1024)
        print(f"Master response: {response.decode()}")
        
        # Send REPLICAOF to configure as slave (simplified)
        writer.write(b"*3\r\n$9\r\nREPLICAOF\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n" % (
            len(master_host), master_host.encode(), len(str(master_port)), master_port
        ))
        await writer.drain()
        
        # Listen for commands from master
        while True:
            data = await reader.read(1024)
            if not data:
                break
            cmd, args = convert_resp(data)
            if cmd:
                try:
                    # Apply replicated commands to local store
                    result = redis_command(cmd, args, {'server_state': server_state})
                    if inspect.iscoroutine(result):
                        await result
                except Exception as e:
                    print(f"Replication error: {e}")
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
    
    # Initialize server_state
    server_state = {
        'role': 'slave' if master_host else 'master',
        'master_host': master_host,
        'master_port': master_port
    }
    
    print(f"Starting server on port {port}")
    if master_host and master_port:
        print(f"Configured as slave of {master_host}:{master_port}")
    else:
        print("Configured as master")
    
    # Start the server
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, server_state),
        "localhost",
        port
    )
    
    # If slave, start replication
    if master_host and master_port:
        asyncio.create_task(start_replication(master_host, master_port, server_state))
    
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())