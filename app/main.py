import argparse
from collections import deque
import os
import asyncio
from .protocol_parser import RedisProtocolParser, Writer
from .rdb import Dbparser
from .commands import handle_echo, handle_ping, handle_get, handle_set,handle_config_get,handle_get_keys,handle_get_info,handle_replconf,handle_psync,handle_rdb_transfer,handle_wait
from .replication import replica_tasks,propagate_commands,datastore


CONFIG = {}
INFO={
    "role":"master",
    "master_replid":"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
    "master_repl_offset":"0"
}
replication_buffer = deque()
replicas = []
rdb_file_path=""
rdb_parser_required = False
def init_rdb_parser(parsing_reqd_flag, rdb_file_path):
    if parsing_reqd_flag and os.path.isfile(rdb_file_path):
        parser =Dbparser(rdb_file_path)
        return parser.kv
    return {}
is_waiting = False


async def handle_client(streamreader, streamwriter):
    addr = streamwriter.get_extra_info('peername')
    print(f"Connected by {addr}")
    replication_offset = 0
    reader,writer = RedisProtocolParser(streamreader),Writer(streamwriter)
    lock = asyncio.Lock()

    while not streamreader.at_eof():
        msg = await reader.parse()
        # print(msg)
        if not msg:
            break  
        # print(f"Received {data}")
        # print("before parsing data")
        # print("data parased")
        command = msg[0].upper()
        if command == 'PING':
            resp = handle_ping(writer)
        elif command == 'ECHO':
            resp = handle_echo(writer, msg)
        elif command == 'GET':
            resp = handle_get(writer, msg, datastore)
        elif command == "SET":
            resp = handle_set(writer, msg, datastore)
            data = writer.serialize(msg)
            replication_offset += reader.get_byte_offset(msg)
            replication_buffer.append(data)
        elif command == "CONFIG":
            resp = handle_config_get(writer,msg,CONFIG)
        elif command == "KEYS":
            resp = handle_get_keys(writer,msg,datastore)
        elif command == "INFO":
            resp = handle_get_info(writer,msg,INFO)
        elif command == "REPLCONF":
            resp = handle_replconf(writer,msg)
        elif command == "PSYNC":
            resp = handle_psync(writer,msg)
            await writer.write_resp(resp)
            val = handle_rdb_transfer(writer,msg)
            await writer.write_resp(val)
            replicas.append((reader,writer))
            # reps = await replicas[0][0].read(1024)
            # print(reps)
            return None
        elif command == "WAIT":
            # print("in wait")
            # async with lock:
            resp = await handle_wait(writer,msg,replicas,replication_offset)
            print(resp)
        

        else:
            resp = b"ERROR unknown command\r\n"
        # print("commands checked")
        # print(resp)

        # print(resp)
        await writer.write_resp(resp)

        # writer.drain()  

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
    if args.replicaof:
        INFO['role']="slave"
        # print("replica_init")
        master_host, master_port = args.replicaof[0].split(" ")
        rep_reader, rep_writer = await asyncio.open_connection(
            master_host, master_port
        )
        # print("replica conn established"+master_host+master_port)
        asyncio.create_task(replica_tasks(rep_reader,rep_writer))
    else:
        asyncio.create_task(propagate_commands(replication_buffer, replicas))

    global datastore
    kv_store = init_rdb_parser(rdb_parser_required, rdb_file_path)
    datastore |= kv_store
    try:
        server = await asyncio.start_server(handle_client, host,port)
        addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
        print(f'Serving on {addrs}')
        async with server:
            await server.serve_forever()
    except Exception as e:
        print(f'error {e} starting server')


if __name__ == "__main__":
    asyncio.run(main())
