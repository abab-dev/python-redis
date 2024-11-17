import argparse
import os
import asyncio
from .protocol_parser import RedisProtocolParser, Writer
from .rdb import Dbparser
from .commands import handle_echo, handle_ping, handle_get, handle_set,handle_config_get,handle_get_keys,handle_get_info


datastore = {}
CONFIG = {}
INFO={
    "role":"master"
}
rdb_file_path=""
rdb_parser_required = False
def init_rdb_parser(parsing_reqd_flag, rdb_file_path):
    if parsing_reqd_flag and os.path.isfile(rdb_file_path):
        parser =Dbparser(rdb_file_path)
        return parser.kv
    return {}


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
        elif command == "CONFIG":
            resp = handle_config_get(writer_obj,msg,CONFIG)
        elif command == "KEYS":
            resp = handle_get_keys(writer_obj,msg,datastore)
        elif command == "INFO":
            resp = handle_get_info(writer_obj,msg,INFO)
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
    parser = argparse.ArgumentParser()
    parser.add_argument( "--dir", type=str, help="The directory where RDB files are stored")
    parser.add_argument( "--dbfilename", type=str, help="The name of the RDB file")
    parser.add_argument( "--port", type=str, help="The port to which this instance will bind")
    parser.add_argument( "--replicaof", nargs=1, help="Specify the host and port")
    args = parser.parse_args()
    if args.dir and args.dbfilename:
        global rdb_file_path, rdb_parser_required
        CONFIG["dir"] = dir = str(args.dir)
        CONFIG["dbfilename"] = filename = str(args.dbfilename)
        rdb_parser_required = True
        rdb_file_path = os.path.join(dir, filename)

    host, port = "127.0.0.1", 6379
    if args.port:
        port = int(args.port)
    global datastore
    kv_store = init_rdb_parser(rdb_parser_required, rdb_file_path)
    datastore |= kv_store
    server = await asyncio.start_server(handle_client, host,port)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
