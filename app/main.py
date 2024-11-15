import asyncio
import socket
from protocol_parser import RedisProtocolParser

async def handle_client(conn):
    while True:
        data = await asyncio.to_thread(conn.recv, 1024)
        if not data:
            break
        parser = RedisProtocolParser()
        parser.feed(data)
        if  parser.retbuf[0] == b'PING':
            await asyncio.to_thread(conn.sendall, b"+PONG\r\n")
        elif parser.retbuf[0] == b'ECHO':
            resp = b'$' + str(len(parser.retbuf[1])).encode('utf-8') + b'\r\n' + parser.retbuf[1] + b'\r\n'
            await asyncio.to_thread(conn.sendall,resp)

    conn.close()

async def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server started. Waiting for clients...")

    while True:
        conn, _ = await asyncio.to_thread(server_socket.accept)
        asyncio.create_task(handle_client(conn))

if __name__ == "__main__":
    asyncio.run(main())