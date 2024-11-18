from io import StringIO

REDIS_REQ_INLINE = '+'
REDIS_REQ_BULK = '$'
REDIS_REQ_MULTIBULK = '*'
REDIS_REQ_ERROR = '-'
REDIS_REQ_STATUS = '+'
REDIS_REQ_INT = ':'
REDIS_REQ_VERB = ''

class RedisProtocolParser:
    def __init__(self):
        self.buf = StringIO()  

    def parse(self, data):
        try: 
            self.buf.write(data.decode('utf-8'))
        except Exception as e:
           status, rest = data.split(b"\xa2") 
           self.buf.write(rest.decode('utf-8'))
        self.buf.seek(0)  

        if self.buf.getvalue().startswith(REDIS_REQ_BULK):
            return self.parse_bulk()
        elif self.buf.getvalue().startswith(REDIS_REQ_MULTIBULK):
            return self.parse_multibulk()
        
        else:
            raise ValueError('Invalid request type')

    def parse_bulk(self):
        line = self.buf.readline()
        length = int(line[1:-2])  
        if length == -1:
            return None  
        data = self.buf.read(length)
        self.buf.read(2)  
        return data

    def parse_multibulk(self):
        arr = []
        while True:
            line = self.buf.readline()
            if not line:
                break
            length = int(line[1:-2])  
            for _ in range(length):
                val = self.parse_bulk()
                arr.append(val)
        return arr
    def read_rdb(self): 
        _ = self.buf.readexactly(1)
        line = self.buf.readline()
        length = int(line)
        if length == -1:
            return b""
        data = self.buf.readexactly(length)
        return data
    def get_byte_offset(self, message) :
        # Returns the byte offset for a RESP command
        # To be used only with RESP Arrays
        offset = 0
        offset += 2 * (2 * len(message) + 1)
        offset += len(str(len(message))) + 1
        for _, val in enumerate(message):
            msg_len = len(val)
            offset += len(str(msg_len)) + 1
            offset += msg_len
        return offset

    

class Writer:
    def __init__(self):
        self.writebuf = b''

    def serialize_str(self, data):
        if data is None:
            return b'$-1\r\n'
        else:
            return f'${len(data)}\r\n{data}\r\n'

    def serialize_error(self, data):
        return f'-{data}\r\n'

    def serialize_integer(self, data):
        return f':{data}\r\n'

    def serialize_array(self, arr):
        response = f'*{len(arr)}\r\n'
        for val in arr:
            if isinstance(val, str):
                response += self.serialize_str(val)
            elif isinstance(val, int):
                response += self.serialize_integer(val)
        return response

    def serialize(self,msg,error=False) :
        if isinstance(msg,list) :
            obj = self.serialize_array(msg)
        elif isinstance(msg,str):
            obj = self.serialize_str(msg)
        elif isinstance(msg,int):
            obj = self.serialize_integer(msg)
        elif error==True:
            obj = self.serialize_error(msg)
        elif isinstance(msg,bytes):
            return msg 
        return obj.encode("utf-8")

    
# p = RedisProtocolParser()
# print(p.parse(b'*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n'))
# print(p.parse(b'+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n'))
# print(dir(StringIO()))
