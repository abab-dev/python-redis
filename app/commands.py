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






