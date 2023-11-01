import socket
import hashlib
from _thread import*
import time

# Server
UDP_IP_OTHER = ''
UDP_PORT_OTHER = 9804
UDP_IP_SELF = ''
UDP_PORT_SELF = 9804

# Protocols
MESSAGE = b"SendSize\n\n"
RESET_MESSAGE = b"SendSize\nReset\n\n"
REQ_SIZE = 1448
SIZE = 0
LINES = 0
WAIT_TIME = 0.006

# Offset queue
ack_queue = dict[int,int]()
# File
file_lines = dict[int,str]()


# f = open("demofile.txt",'w')

# Message to bytes
def msg_to_bytes(offset: int,byte: int) -> bytes:
    temp: str = f"Offset: {offset}\nNumBytes: {byte}\n\n"
    return temp.encode()

# Size receiving protocol
def recv_size(server:socket.socket,reset : bool = False) -> None:
    print("Receiving size.")
    global SIZE
    while True:
        try:
            if reset:
                server.sendto(RESET_MESSAGE,(UDP_IP_OTHER, UDP_PORT_OTHER))
            else:
                server.sendto(MESSAGE,(UDP_IP_OTHER, UDP_PORT_OTHER))
            data = server.makefile("r", encoding="utf8", newline="\n")
            # raw,addr = server.recvfrom(REQ_SIZE)
            raw = data.readline()
            SIZE = int(raw.split(':')[1])
            print(f"Received a the size of file : {SIZE}")
            break
        except:
            # print("Size re quest not sent or packet was dropped.")
            pass
    print("Successful received the size of file.")

# Queue of block not received
def initialize_queue(size:int,packet_size:int) -> None:
    print("Initializing queue.")
    global ack_queue,LINES
    cnt = 0
    increment:int = 0
    while (increment < size):
        ack_queue[increment] = packet_size
        increment += packet_size
        LINES += 1
    increment -= packet_size
    ack_queue[increment] = size-increment
    print("Queue initialized!")

# Hash
def MD5_Hash() -> str:
    print("Generating hash.")
    global REQ_SIZE, SIZE
    req_size:int = 0
    byte_stream:str = ""
    while req_size < SIZE:
        byte_stream += file_lines[req_size]
        req_size += REQ_SIZE
    byte_stream_:str = hashlib.md5(byte_stream.encode()).hexdigest()
    print("Hash generated!")
    print("Hash ->",byte_stream_)
    return byte_stream_

# Submission protocol
def submit(server:socket.socket) -> None:
    print("Submitting messages...")
    data_ = MD5_Hash()
    msg = f"Submit: Mayank@Mayank\nMD5: {data_}\n\n"
    server.sendto(msg.encode(),(UDP_IP_OTHER, UDP_PORT_OTHER))
    print("Successful successful submitted the hash!")

# Receiving messages in parallel
def recv_msg(server:socket.socket,offset:int) -> bool:
    global ack_queue, file_lines
    try:
        data,addr = server.recvfrom(2000)
        data = data.decode().split('\n',3)
        _,offset_ = data[0].split(": ")
        byte_to_string_stream :str = data[3]
        if offset == int(offset_):
            # f.write(f"\n{offset} :\n")
            # f.write(byte_to_string_stream)
            file_lines[int(offset)] = byte_to_string_stream
            return True
        return False
    except:
        # print(f"{offset} error.")
        return False

# Requesting messages in parallel
def req_msg(server:socket.socket) -> None:
    print("Requesting messages...")
    global ack_queue
    for offset,size in ack_queue.items():
        # print(f"Asking for {offset}.")
        time.sleep(WAIT_TIME)
        msg: bytes = msg_to_bytes(offset,size)
        while True:
            try:
                server.sendto(msg,(UDP_IP_OTHER, UDP_PORT_OTHER))
                if not recv_msg(server,offset):
                    # print("Oops! Message got dropped?")
                    continue
                # print("Successful requested the messages!")
                break
            except: pass
    print("All message requested!")

# Main block
with socket.socket(family=socket.AF_INET,type=socket.SOCK_DGRAM) as server:
    # t = time.time()
    server.settimeout(WAIT_TIME)
    recv_size(server)                               # Get size
    initialize_queue(SIZE,REQ_SIZE)                 # Initialize DS
    req_msg(server)
    submit(server)                                        # Perform submission
    # print(time.time()-t)
    reply = server.makefile("r", encoding="utf8", newline="\n")
    try:
        for replies in reply:
            print(replies,end='')
    except:
        pass
