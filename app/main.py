import asyncio
import socket

async def handle_client(conn):
    while True:
        data = await asyncio.to_thread(conn.recv, 1024)
        if not data:
            break
        await asyncio.to_thread(conn.sendall, b"+PONG\r\n")
    conn.close()

async def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server started. Waiting for clients...")

    while True:
        conn, _ = await asyncio.to_thread(server_socket.accept)
        asyncio.create_task(handle_client(conn))

if __name__ == "__main__":
    asyncio.run(main())