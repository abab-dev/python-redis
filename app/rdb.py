from .time_utils import EXPIRY_DEFAULT 
class Dbparser():
    def __init__(self,path) -> None:
        try:
            self.db = open(path,"rb")
        except:
            print("cant open")
        magic_string = self.db.read(5).decode("utf-8")
        assert magic_string == "REDIS"
        self.db.read(4)
        self.parse_dict()
        self.db.read(2)
        self.db.read(1)
        self.parse_length_encoded_int()
        self.parse_length_encoded_int()
        self.kv = self.parse_key_dict()
        # print(str(self.db.readline()))
    
        # for line in self.db.readlines(5):
            # print(line.decode("utf-8")) 

    def parse_length_encoded_int(self) :
        determinant = self.db.read(1)[0]
        determinant_bits = bin(determinant)[2:].zfill(8)

        special_format = False
        match determinant_bits[:2]:
            case "00":
                length = determinant & 0b00111111
            case "01":
                next_byte = self.db.read(1)[0]
                first_byte = determinant & 0b00111111
                length = (first_byte << 8) | next_byte
            case "10":
                length = int.from_bytes(self.db.read(4), byteorder="big")
            case "11":
                # The next object is encoded in a special format.
                # The remaining 6 bits indicate the format.
                format = determinant & 0b00111111
                special_format = True
                match int(format):
                    case 0:
                        length = 1
                    case 1:
                        length = 2
                    case 2:
                        length = 4
                    case 3:
                        length = -1
                    case _:
                        raise ValueError(
                            "Unknown format for Length Encoded Int case '11'"
                            f" : {int(format)}"
                        )
            case _:
                raise ValueError(
                    "Unknown encoding for Length Encoded Int :"
                    f" {determinant_bits[2:]}"
                )

        return special_format, length

    def parse_encoded_string(self) :
        special_format, length = self.parse_length_encoded_int()
        if not special_format:
            return self.db.read(length).decode()
        else:
            return str(
                int.from_bytes(self.db.read(length), byteorder="big")
            )

    def parse_dict(self):
        d = {}
        while self.db.peek(1)[:1] == b"\xfa":
            self.db.read(1)  # Skip
            key = self.parse_encoded_string()
            value = self.parse_encoded_string()
            d[key] = value
        return d

    def parse_key_dict(self) :
        d = {}
        while self.db.peek(1)[:1] != b"\xff":
            if self.db.peek(1)[:1] == b"\xfc":
                # "expiry time in ms", followed by 8 byte unsigned long
                self.db.read(1)  # Skip
                expiry = int.from_bytes(
                    self.db.read(8), byteorder="little"
                )
            else:
                expiry = EXPIRY_DEFAULT
            _ = self.db.read(1)  # value_type
            key = self.parse_encoded_string()
            value = self.parse_encoded_string()
            d[key] = (value, expiry)

        return d


# d = Dbparser(r"/home/abh/Desktop/codecrafters-redis-python/dump.rdb")
# print(d.kv)
