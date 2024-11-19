from .protocol_parser import RedisProtocolParser,Writer
from io import BytesIO
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
                    _, rep_writer = replica
                    await rep_writer.write_resp(cmd)
        await asyncio.sleep(0.1)

async def replica_tasks(rep_reader,rep_writer):
    reader,writer = RedisProtocolParser(rep_reader),Writer(rep_writer)
    print("replica_start")
    data =  await writer.write_resp(['PING'])
    resp = await reader.parse()
    print(data)

    resp =  await writer.write_resp(['REPLCONF','listening-port','6380'])
    data = await reader.parse()
    print(data)

    resp =  await writer.write_resp(["REPLCONF", "capa", "psync2", "capa", "psync2"])
    data = await reader.parse()
    print(data)

    resp =  await writer.write_resp(["PSYNC", "?", "-1"])
    data = await reader.parse()
    print(data)

    rdb = await reader.read_rdb()
    print(rdb)
    
    # resp = writer_obj.serialize([])
    
    offset = 0  # Count of processed bytes
    while True:
        try:
            msg  =  await reader.parse()
            # print(msg)
            if(not msg):
                continue
            print("Received from master :", msg)
        except (ConnectionResetError) as err:
            print(err)
            await rep_writer.close()
            return
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
                await writer.write_resp(response)
               
            case _:
                # print("rest of the match")
                # for m in msg:
                #     replication_buffer.append(m)
                # msg= []
                pass
                    
                    
                    # return(b'+1\r\n')
                    
            
        bytes_to_process = reader.get_byte_offset(msg)
        offset += bytes_to_process
