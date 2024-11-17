from .protocol_parser import RedisProtocolParser,Writer
from time import sleep
from .time_utils import create_ts
async def propagate_commands(
    replication_buffer,
    replicas
): 
    WAIT_TIME = 0.125  # seconds
    while True:
        if len(replicas) != 0:
            if len(replication_buffer) != 0:
                cmd = replication_buffer.popleft()
                for replica in replicas:
                    _, w = replica
                    se = Writer()
                    await w.write(se.serialize(cmd))
        sleep(WAIT_TIME)
async def replica_tasks(rep_reader,rep_writer):
    parser = RedisProtocolParser() 
    writer_obj = Writer()
    print("replica_start")
    data =  writer_obj.serialize(['PING'])
    rep_writer.write(data)
    resp = await rep_reader.read(1024)
    print(data)

    resp =  writer_obj.serialize(['REPLCONF','listening-port','6380'])
    rep_writer.write(resp)
    data = await rep_reader.read(1024)
    print(data)

    resp =  writer_obj.serialize(["REPLCONF", "capa", "psync2", "capa", "psync2"])
    rep_writer.write(resp)
    data = await rep_reader.read(1024)
    print(data)

    resp =  writer_obj.serialize(["PSYNC", "?", "-1"])
    rep_writer.write(resp)
    data = await rep_reader.read(1024)
    print(data)
    rdb = await rep_reader.read(1024)
    data = parser.read_rdb(rdb)
    
    print(data)
    global datastore
    offset = 0  # Count of processed bytes
    while True:
        try:
            msg = await parser.parse()
            print("Received from master :", msg)
        except (ConnectionResetError) as err:
            print(err)
            await writer_obj.close()
            return
        command = msg[0].upper()
        match command:
            case "SET":
                key, value = msg[1], msg[2]
                datastore[key] = (
                    value,
                    create_ts([]),
                )  # No active expiry
            case "REPLCONF":
                # Master won't send any other REPLCONF message apart from
                # GETACK.
                response = ["REPLCONF", "ACK", str(offset)]
                await rep_writer.write(writer_obj.serialize(response))
            case _:
                pass
        bytes_to_process = parser.get_byte_offset(msg)
        offset += bytes_to_process
