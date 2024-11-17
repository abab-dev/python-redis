from .protocol_parser import RedisProtocolParser,Writer
from time import sleep
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
        await sleep(WAIT_TIME)
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
    return
