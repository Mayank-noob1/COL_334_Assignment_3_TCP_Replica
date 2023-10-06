import socket
import hashlib
from _thread import*

# Server
UDP_IP_OTHER = "vayu.iitd.ac.in"
UDP_PORT_OTHER = 9801
UDP_IP_SELF = "127.0.0.1"
UDP_PORT_SELF = 5005

# Protocols
MESSAGE = b"SendSize\n\n"
RESET_MESSAGE = b"SendSize\nReset\n\n"
REQ_SIZE = 1448
SIZE = 0
LINES = 0

# Offset queue
ack_queue = dict[int,int]
# File
file_lines = dict[int,str]

# Message to bytes
def msg_to_bytes(offset: int,byte: int) -> bytes:
    temp: str = f"Offset:{offset}\nNumBytes:{byte}\n\n"
    return temp.encode()

# Size receiving protocol
def recv_size(server:socket.socket,reset : bool = False) -> None:
    print("Receiving size.")
    global SIZE
    if reset:
        server.sendto(RESET_MESSAGE,(UDP_IP_OTHER, UDP_PORT_OTHER))
    else:
        server.sendto(MESSAGE,(UDP_IP_OTHER, UDP_PORT_OTHER))
    data = server.makefile("r", encoding="utf8", newline="\n")
    SIZE = int(data.readline())
    print("Successful received the size of file.")

# Queue of block not received
def initialize_queue(size:int,packet_size:int) -> None:
    print("Initializing queue.")
    global ack_queue,LINES
    cnt = 0
    increment = 0
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
    return byte_stream_

# Submission protocol
def submit(server:socket.socket) -> None:
    print("Submitting messages...")
    data_ = MD5_Hash()
    msg = f"Submit:Mayank@Mayank\nMD5:{data_}\n\n"
    server.sendto(msg.encode(),(UDP_IP_OTHER, UDP_PORT_OTHER))
    print("Successful successful submitted the hash!")

# Receiving messages in parallel
def recv_msg(server:socket.socket) -> None:
    print("Receiving messages...")
    global ack_queue, file_lines
    while len(file_lines) != LINES:
        data = server.makefile("r", encoding="utf8", newline="\n")
        _,offset_ = data.readline().split(':')
        _,numbytes_ = data.readline().split(':')
        _ = data.readline()
        byte_to_string_stream :str = data.readline()

        if int(offset_) not in file_lines:
            file_lines[int(offset_)] = byte_to_string_stream
            ack_queue.pop(int(offset_))
    print("Successful received all the messages!")

# Requesting messages in parallel
def req_msg(server:socket.socket) -> None:
    print("Requesting messages...")
    global ack_queue
    while len(ack_queue):
        for offset,size in ack_queue:
            msg: bytes = msg_to_bytes(offset,size)
            server.sendto(msg,(UDP_IP_OTHER, UDP_PORT_OTHER))
    print("Successful requested all the messages!")

# Main block
with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as server:
    server.bind((UDP_IP_SELF,UDP_PORT_SELF))        # Binding a receiving port
    recv_size(server)                               # Get size
    initialize_queue(SIZE,REQ_SIZE)                 # Initialize DS
    start_new_thread(recv_msg,(server))             # Start receiving messages
    start_new_thread(req_msg,(server))              # Start requesting messages
    while len(file_lines) != LINES: pass            # Wait till whole file is reassembled
    submit()                                        # Perform submission
    reply = server.makefile("r", encoding="utf8", newline="\n")
    while reply:
        print(reply.readline())
