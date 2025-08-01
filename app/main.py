import socket
import asyncio
import argparse
import inspect
from app.convert_commands import convert_resp
from app.commands import redis_command

async def handle_client(reader, writer):
    # Client-specific transaction state
    client_state = {
        'multi_event': asyncio.Event(),
        'exec_event': [],
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
                    # Pass client-specific state to redis_command
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

async def main():
    print("Logs from your program will appear here!")
    parser = argparse.ArgumentParser(description="Redis server with custom port")
    parser.add_argument("--port", type=int, default=6379, help="Port to run the server on")
    args = parser.parse_args()
    port = args.port
    print(f"Starting server on port {port}")
    print("Logs from your program will appear here!")
    server = await asyncio.start_server(handle_client, "localhost", port)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())