import socket
import asyncio, inspect
from app.convert_commands import convert_resp
from app.commands import redis_command

async def handle_client(reader, writer):
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            cmd, args = convert_resp(data)
            if not cmd:
                resp = "-ERR invalid RESP format\r\n"
            else:
                result = redis_command(cmd, args)
                # if the command func was async we get a coroutine back:
                if inspect.iscoroutine(result):
                    result = await result
                resp = result
            writer.write(resp.encode() if isinstance(resp, str) else resp)
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    print("Logs from your program will appear here!")
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())