from asyncio import StreamReader,StreamWriter
REDIS_REQ_INLINE = '+'
REDIS_REQ_BULK = '$'
REDIS_REQ_MULTIBULK = '*'
REDIS_REQ_ERROR = '-'
REDIS_REQ_STATUS = '+'
REDIS_REQ_INT = ':'
REDIS_REQ_VERB = ''

class RedisProtocolParser:
    def __init__(self,reader):
        self.reader= reader 

    async def parse(self):
        msg= await self.reader.readexactly(1)
        command = msg.decode('utf-8')
        match command:
            case "+":  
                string = await  self.parse_string()
                return string
            case "-":  
                err = await self.parse_error()
                return err
            case ":": 
                num = await self.parse_integer()
                return num
            case "$":
                string =  await self.parse_bulk()  # type: ignore
                return string
            case "*":
                arr = await self.parse_multibulk()
                return arr
            case _:
                raise RuntimeError(f"Unknown payload identifier : {msg}")
    async def parse_string(self):
        data = await self.read_line()
        return data
    async def parse_error(self):
        data = await self.read_line()
        return data

    async def parse_integer(self):
        data = await self.read_line()
        return int(data)
    async def parse_bulk(self):
        line = await self.read_line()
        length = int(line)  
        if length == -1:
            return None  
        data = await self.read_until(length+2)
        return data
    async def read_line(self) :
        data = await self.reader.readuntil(b"\r\n")
        # print("data recieved is")
        # print(data)
        return data[:-2].decode()

    async def read_until(self, n) :
        data = await self.reader.readexactly(n)
        return data[:-2].decode()


    async def parse_multibulk(self):
        arr = []
        line = await self.read_line()
        # print(line.encode('utf-8'))
        length = int(line) 
        # print("lenght is " , length)
        for _ in range(length):
            val = await self.parse()
            # print("parsed value is ", val)
            arr.append(val)
        return arr


    async def read_rdb(self): 
        _ = await self.reader.readexactly(1)
        line = await self.read_line()
        length = int(line)
        if length == -1:
            return b""
        data = await self.reader.readexactly(length)
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
    def __init__(self,writer):
        self.writer= writer 

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
    
    async def write_resp(self,msg):
        data = self.serialize(msg)
        print(data)
        if not data:
            return 
        self.writer.write(data)

    
# p = RedisProtocolParser()
# # print()
# print(p.parse(b'*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n'))
# print(p.parse(b'+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n'))
# print(dir(StringIO()))
