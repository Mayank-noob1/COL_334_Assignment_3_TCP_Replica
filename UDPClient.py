import socket
import hashlib

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
    global SIZE
    if reset:
        server.sendto(RESET_MESSAGE,(UDP_IP_OTHER, UDP_PORT_OTHER))
    else:
        server.sendto(MESSAGE,(UDP_IP_OTHER, UDP_PORT_OTHER))
    data = server.makefile("r", encoding="utf8", newline="\n")
    SIZE = int(data.readline())
# Queue of block not received
def initialize_queue(size:int,packet_size:int) -> None:
    global ack_queue
    cnt = 0
    increment = 0
    while (increment < size):
        ack_queue[increment] = packet_size
        increment += packet_size
    increment -= packet_size
    ack_queue[increment] = size-increment
# Hash
def MD5_Hash() -> str:
    global REQ_SIZE, SIZE
    req_size:int = 0
    byte_stream:str = ""
    while req_size < SIZE:
        byte_stream += file_lines[req_size]
        req_size += REQ_SIZE
    byte_stream_:str = hashlib.md5(byte_stream.encode()).hexdigest()
    return byte_stream_
# Submission protocol
def submit(server:socket.socket):
    data_ = MD5_Hash()
    msg = f"Submit:Mayank@Mayank\nMD5:{data_}\n\n"
    server.sendto(msg.encode(),(UDP_IP_OTHER, UDP_PORT_OTHER))
# Main block
with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as server:
    server.bind((UDP_IP_SELF,UDP_PORT_SELF))
    recv_size(server)
    initialize_queue(SIZE,REQ_SIZE)
    while len(ack_queue):
        for offset,size in ack_queue:
            msg: bytes = msg_to_bytes(offset,size)
            while True:
                server.sendto(msg,(UDP_IP_OTHER, UDP_PORT_OTHER))

                data = server.makefile("r", encoding="utf8", newline="\n")
                _,offset_ = data.readline().split(':')
                _,numbytes_ = data.readline().split(':')
                empty_ = data.readline()
                byte_to_string_stream :str = data.readline()

                if int(offset_) not in file_lines:
                    file_lines[int(offset_)] = byte_to_string_stream
                    ack_queue.pop(int(offset_))
                    break
    submit()
    reply = server.makefile("r", encoding="utf8", newline="\n")
    while reply:
        print(reply.readline())