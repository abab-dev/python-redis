import asyncio
import socket
# from protocol_parser import RedisProtocolParser
REDIS_REQ_INLINE = b'+'
REDIS_REQ_BULK = b'$'
REDIS_REQ_MULTIBULK = b'*'
REDIS_REQ_ERROR = b'-'
REDIS_REQ_STATUS = b'+'
REDIS_REQ_INT = b':'
REDIS_REQ_VERB = b''

class RedisProtocolParser:
    def __init__(self):
        self.state = 'idle'
        self.buf = b''
        self.retbuf = [] 

    def feed(self, data):
        self.buf += data
        self.parse()

    def parse(self):
        while self.buf:
            if self.state == 'idle':
                self.parse_request_type()
            elif self.state == 'bulk':
                self.parse_bulk()
            elif self.state == 'multibulk':
                self.parse_multibulk()
            elif self.state == 'error':
                self.parse_error()
            elif self.state == 'status':
                self.parse_status()
            elif self.state == 'int':
                self.parse_int()
            elif self.state == 'verb':
                self.parse_verb()

    def parse_request_type(self):
        if self.buf.startswith(REDIS_REQ_INLINE):
            self.state = 'inline'
            self.buf = self.buf[1:]
        elif self.buf.startswith(REDIS_REQ_BULK):
            self.state = 'bulk'
            self.buf = self.buf[1:]
        elif self.buf.startswith(REDIS_REQ_MULTIBULK):
            self.state = 'multibulk'
            self.buf = self.buf[1:]
        elif self.buf.startswith(REDIS_REQ_ERROR):
            self.state = 'error'
            self.buf = self.buf[1:]
        elif self.buf.startswith(REDIS_REQ_STATUS):
            self.state = 'status'
            self.buf = self.buf[1:]
        elif self.buf.startswith(REDIS_REQ_INT):
            self.state = 'int'
            self.buf = self.buf[1:]
        elif self.buf.startswith(REDIS_REQ_VERB):
            self.state = 'verb'
            self.buf = self.buf[1:]
        else:
            raise ValueError('Invalid request type')

    def parse_bulk(self):
        length_str, self.buf = self.buf.split(b'\r\n', 1)
        length = int(length_str[1:])
        # print(length)
        if len(self.buf) < length + 2:  # +2 for the trailing \r\n
            return
        data = self.buf[:length]
        self.buf = self.buf[length + 2:]  # Skip the data and the trailing \r\n
        self.state = 'idle'
        return data

    def parse_multibulk(self):
        length_str, self.buf = self.buf.split(b'\r\n', 1)
        length = int(length_str)
        # print('Multibulk length:', length)
        for _ in range(length):
            if self.buf.startswith(REDIS_REQ_BULK):
                self.state = 'bulk'
                val = self.parse_bulk()
                self.retbuf.append(val)
                
            else:
                raise ValueError('Expected bulk string in multibulk')
        self.state = 'idle'

    def parse_error(self):
        data, self.buf = self.buf.split(b'\r\n', 1)
        self.state = 'idle'
        print('Error:', data)

    def parse_status(self):
        data, self.buf = self.buf.split(b'\r\n', 1)
        self.state = 'idle'
        print('Status:', data)

    def parse_int(self):
        data, self.buf = self.buf.split(b'\r\n', 1)
        self.state = 'idle'
        print('Int:', data)

    def parse_verb(self):
        data, self.buf = self.buf.split(b'\r\n', 1)
        self.state = 'idle'
        print('Verb:', data)

# parser = RedisProtocolParser()
# parser.feed(b'*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n')
# print(parser.retbuf)

async def handle_client(conn):
    while True:
        data = await asyncio.to_thread(conn.recv, 1024)
        if not data:
            break
        parser = RedisProtocolParser()
        parser.feed(data)
        if  parser.retbuf[0] == b'PING':
            await asyncio.to_thread(conn.sendall, b"+PONG\r\n")
        elif parser.retbuf[0] == b'ECHO':
            resp = b'$' + str(len(parser.retbuf[1])).encode('utf-8') + b'\r\n' + parser.retbuf[1] + b'\r\n'
            await asyncio.to_thread(conn.sendall,resp)

    conn.close()

async def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server started. Waiting for clients...")

    while True:
        conn, _ = await asyncio.to_thread(server_socket.accept)
        asyncio.create_task(handle_client(conn))

if __name__ == "__main__":
    asyncio.run(main())