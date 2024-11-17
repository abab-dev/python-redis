from .protocol_parser import RedisProtocolParser,Writer
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
    return
