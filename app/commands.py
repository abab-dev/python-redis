def handle_echo(writer,arr):
    return writer.serialize(arr[1:])
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
    try:
        value = datastore.get(key,"kya_kya_dekhana_pad_raha_hai")
        return writer.serialize(value)
    except:
        return writer.serialize("key does not exist",e=True)






