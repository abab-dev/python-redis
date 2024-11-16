import asyncio
from .protocol_parser import RedisProtocolParser, Writer
from .commands import handle_echo, handle_ping, handle_get, handle_set

datastore = {}

async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Connected by {addr}")

    while not reader.at_eof():
        data = await reader.read(1024)
        if not data:
            break  
        # print(f"Received {data}")
        parser = RedisProtocolParser()
        writer_obj = Writer()
        # print("before parsing data")
        msg = parser.parse(data)
        # print("data parased")
        command = msg[0].upper()
        if command == 'PING':
            resp = handle_ping(writer_obj)
        elif command == 'ECHO':
            resp = handle_echo(writer_obj, msg)
        elif command == 'GET':
            resp = handle_get(writer_obj, msg, datastore)
        elif command == "SET":
            resp = handle_set(writer_obj, msg, datastore)
        else:
            resp = b"ERROR unknown command\r\n"
        # print("commands checked")
        # print(resp)

        writer.write(resp)
        await writer.drain()  

    # print("Close the connection")
    writer.close()
    await writer.wait_closed()

async def main():
    server = await asyncio.start_server(handle_client, '127.0.0.1', 6379)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
