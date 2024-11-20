from .time_utils import create_ts,validate_ts,EXPIRY_DEFAULT
import binascii
import time
import asyncio
def handle_echo(writer,arr):
    resp = arr[1]
    return writer.serialize(resp)
def handle_ping(writer):
    resp = b'+PONG\r\n'
    return writer.serialize(resp)
def handle_set(writer,msg,datastore):
    key,value = msg[1],msg[2]
    datastore[key]= value,create_ts(msg)
    resp = b"+OK\r\n"
    return writer.serialize(resp)
def handle_get(writer,msg,datastore):
    # print("replica_datastore"+"\n")
    # print(datastore)
    # print("replica_check_init")
    key = msg[1]
    # print("replica_msg_key ",msg)
    default_value = None,EXPIRY_DEFAULT
    value,expiry_ts = datastore.get(key,default_value)
    print(value,expiry_ts)
    expired = validate_ts(datastore,key,expiry_ts)
    print(expired)
    if(expired or value==None):
        return b'$-1\r\n'
    return writer.serialize(value)
def handle_config_get(writer,msg,config):
    key = msg[2]
    value = config.get(key,None)
    return writer.serialize([key,value])
def handle_get_keys(writer,msg,datastore):
    key = msg[1]
    assert key == "*"
    key_list = list(datastore.keys())
    print()
    return  writer.serialize(key_list)
def handle_get_info(writer,msg,info):
    if(msg[1]=="replication"):
        resp = "\r\n".join(f"{key}:{value}" for key, value in info.items())
        return writer.serialize(resp)
def handle_replconf(writer,msg):
    return b"+OK\r\n"
def handle_psync(writer, msg): 
    response = f"FULLRESYNC 524544495330303131fa0972656469732d76657205372e322e30 0"
    return writer.serialize(response)
def handle_rdb_transfer(writer,msg):
    hex_str = (
        "524544495330303131fa0972656469732d76657205372e322e30"
        "fa0a72656469732d62697473c040fa056374696d65c26d08bc65"
        "fa08757365642d6d656dc2b0c41000fa08616f662d62617365c0"
        "00fff06e3bfec0ff5aa2"
    )
    try:
        # Decode the hex string to bytes
        bytes_data = binascii.unhexlify(hex_str)
    except binascii.Error as e:
        print(f"Encountered {e} while decoding hex string")
        return None  # Adjust based on how you want to handle errors

    resp = b"$" + str(len(bytes_data)).encode() + b"\r\n"
    msg = resp + bytes_data

    return writer.serialize(msg)
async def handle_wait(writer,msg,replicas,replication_offset):
    updated_replicas = 0
    start_time = time.time()
    print(f"timer started at {start_time}")
    await asyncio.sleep(0.125)
    num_replicas,timeout = int(msg[1]),int(msg[2])
    if num_replicas == 0:
        resp = len(replicas)
    else:
        for repl_reader,repl_writer in replicas:
            await repl_writer.write_resp(['REPLCONF','GETACK','*'])
        print("sent ack to all replicas")

        for repl_reader,repl_writer in replicas:
            try:
                recieved_response = await asyncio.wait_for(repl_reader.parse(),timeout=0.125)
                print("recieved_response ", recieved_response)
                if(
                    recieved_response 
                    and recieved_response[0] == 'REPLCONF'
                    and recieved_response[1] ==  'ACK'
                ):
                    local_offset = int(recieved_response[2])
                    if local_offset >= replication_offset:
                        print("local_offset",local_offset)
                        print("replication_offset",replication_offset)
                        updated_replicas+=1
            except asyncio.TimeoutError:
                print("Timeout Expird")
    resp = updated_replicas
    print('out of for loop of handle_wait response is',resp)
    end_time = time.time()
    # print(f'end time is {end_time} and loffset {local_offset} and repl_offset {replication_offset}')
    elapsed_time = (end_time-start_time)*1000
    if resp<num_replicas and replication_offset!=0:
        t = max(0,timeout-elapsed_time)
        print(f"waiting for {t} ms")
        await asyncio.sleep(t/1000)
    return writer.serialize(resp)





    

    








