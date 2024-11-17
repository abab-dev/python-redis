from .protocol_parser import RedisProtocolParser,Writer
async def replica_tasks(rep_reader,rep_writer):
    parser = RedisProtocolParser() 
    writer_obj = Writer()
    while rep_reader.at_eof():
        resp =  writer_obj.serialize(['PING'])
        await rep_writer.write(resp)
        data = await rep_reader.read(1024)
        print(parser.parse(data))

        resp =  writer_obj.serialize(['REPLCONF','listening-port','6380'])
        await rep_writer.write(resp)
        data = await rep_reader.read(1024)
        print(parser.parse(data))

        resp =  writer_obj.serialize([["REPLCONF", "capa", "psync2", "capa", "psync2"]])
        await rep_writer.write(resp)
        data = await rep_reader.read(1024)
        print(parser.parse(data))
