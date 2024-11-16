def handle_echo(writer,arr):
    return writer.serialize(arr[1:])
def handle_ping(writer):
    resp = b'+PONG\r\n'
    return writer.serialize(resp)
def handle_set(writer,msg,datastore):
    key,value = msg[1],msg[2]
    datastore[key]= value
    return writer.serialize("OK")
def handle_get(writer,msg,datastore):
    key = msg[1]
    value = datastore.get(key,None)
    return writer.serialize(value)




# p = RedisProtocolParser()
# w = Writer()

# o = p.parse(b'*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n')
# print(o)
# handle_set(w,o,{})
# print(w.serialize("OK"))
# print(handle_ping(w1))
# print(handle_set(w1,["SET",'1',2],{}))
    
