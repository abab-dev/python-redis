from .time_utils import create_ts,validate_ts,EXPIRY_DEFAULT
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
    key = msg[1]
    default_value = None,EXPIRY_DEFAULT
    value,expiry_ts = datastore.get(key,default_value)
    expired = validate_ts(datastore,key,expiry_ts)
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
async def handle_psync(writer, msg): 
    response = f"FULLRESYNC 524544495330303131fa0972656469732d76657205372e322e30 0"
    return await writer.serialize(response)
    








