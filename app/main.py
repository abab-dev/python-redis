__package__  = 'app'
import asyncio
import socket
from .protocol_parser import RedisProtocolParser,Writer
from .commands import handle_echo,handle_ping,handle_get,handle_set
datastore={}

async def handle_client(conn):
    
    while True:
        data = await asyncio.to_thread(conn.recv, 1024)
        if not data:
            break
        parser = RedisProtocolParser()
        writer = Writer()
        msg = parser.parse(data)
        command = msg[0].upper()
        if  command == 'PING':
            resp = handle_ping(writer) 
        elif command == 'ECHO':
            resp = handle_echo(writer,msg) 
        elif command == 'GET':
            resp = handle_get(writer,msg,datastore)
        elif command == "SET":
            resp = handle_set(writer,msg,datastore)


        return await asyncio.to_thread(conn.sendall,resp)
    conn.close()

async def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server started. Waiting for clients...")

    while True:
        conn, _ = await asyncio.to_thread(server_socket.accept)
        asyncio.create_task(handle_client(conn))

if __name__ == "__main__":
    asyncio.run(main())