def handle_echo(writer,arr):
    resp = arr[1]
    return writer.serialize(resp)
def handle_ping(writer):
    resp = b'+PONG\r\n'
    return writer.serialize(resp)
def handle_set(writer,msg,datastore):
    key,value = msg[1],msg[2]
    datastore[key]= value
    resp = b"+OK\r\n"
    return writer.serialize(resp)
def handle_get(writer,msg,datastore):
    key = msg[1]
    value = datastore.get(key,None)
    if(value == None):
        return writer.serialize("does not exits",e=True)
    return writer.serialize(value)






