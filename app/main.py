import socket
import asyncio
from app.convert_commands import convert_resp
from app.commands import redis_command

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        while True:
            request: bytes = await reader.read(1024) #read up to 1024
            if not request:
                break
            commands,args = convert_resp(request)
            print(commands,args)
            if not commands:
                writer.write("-ERR invalid RESP format\r\n".encode())
                continue
            if isinstance(commands,str):
                writer.write(redis_command(commands,args))
            else:
                for command,arg in zip(commands,args):
                    writer.write(redis_command(command,arg))
            await writer.drain()
    except Exception as e:
        print(f"Error handling client: {e}")
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