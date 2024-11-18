from .protocol_parser import RedisProtocolParser,Writer
from .time_utils import create_ts
import asyncio
datastore={}
async def propagate_commands(
    replication_buffer,
    replicas
): 
    while True:
        # print("replication_task_runningi")
        if len(replicas) != 0:
            if len(replication_buffer) != 0:
                cmd = replication_buffer.popleft()
                for replica in replicas[:]:
                    _, w = replica
                    se = Writer()
                    w.write(se.serialize(cmd))
        await asyncio.sleep(0.125)

async def replica_tasks(rep_reader,rep_writer):
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
    rep_writer.write(writer_obj.serialize(resp))
    rdb = await rep_reader.read(1024)
    print(rdb)
    
    offset = 0  # Count of processed bytes
    while True:
        try:
            resp =  await rep_reader.read(1024)
            print(resp)
            parser = RedisProtocolParser()
            msg = parser.parse(resp)
            if(not msg):
                continue
            print("Received from master :", msg)
        except (ConnectionResetError) as err:
            print(err)
            await rep_writer.close()
            return
        while msg:
            command = msg[0].upper()
            match command:
                case "SET":
                    key, value = msg[1], msg[2]
                    datastore[key] = (
                        value,
                        create_ts([]),
                    )  # No active expiry
                    print(datastore)
                case "REPLCONF":
                    # Master won't send any other REPLCONF message apart from
                    # GETACK.
                    response = ["REPLCONF", "ACK", str(offset)]
                    await rep_writer.write(writer_obj.serialize(response))
                case _:
                    pass
            msg = msg[3:]
        bytes_to_process = parser.get_byte_offset(msg)
        offset += bytes_to_process
