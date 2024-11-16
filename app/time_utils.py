import time

EXPIRY_DEFAULT = 0

def create_ts(msg):
    if len(msg) > 3:
        opt, exp = msg[3], msg[4]
        expiry = int(exp)
        if opt == 'px':
            expiry_ts = int(time.time() * 1000) + expiry 
        else:
            raise ValueError("Invalid expiry option")
    else:
        expiry_ts = EXPIRY_DEFAULT
    return expiry_ts

def validate_ts(datastore, key, expiry_ts):
    current_ts = int(time.time() * 1000)
    if expiry_ts == EXPIRY_DEFAULT:
        return False  
    elif key in datastore and current_ts > expiry_ts:
        del datastore[key]
        return True
    return False

# datastore = {}
# msg = ['SET', 'key', 'value', 'px', '10']  
# expiry_ts = create_ts(msg)
# datastore['key'] = 'value'

# time.sleep(1)
# if validate_ts(datastore, 'key', expiry_ts):
#     print("Key expired and deleted")
# else:
#     print("Key is still valid")
