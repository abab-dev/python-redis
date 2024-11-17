from .protocol_parser import RedisProtocolParser,Writer
async def replica_tasks(rep_reader,rep_writer):
    parser = RedisProtocolParser() 
    writer_obj = Writer()
    while True:
        resp =  writer_obj.serialize(['PING'])
        await rep_writer.write(resp)
        data = await rep_reader.read(1024)
        print(parser.parse(data))

