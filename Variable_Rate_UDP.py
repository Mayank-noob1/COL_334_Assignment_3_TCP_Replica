import socket,hashlib,random,threading,time
from _thread import*

# Server
UDP_IP_OTHER = 'vayu.iitd.ac.in'
UDP_PORT_OTHER = 9802
UDP_IP_SELF = ''
UDP_PORT_SELF = 9810

# Protocols
MESSAGE = b"SendSize\n\n"
RESET_MESSAGE = b"SendSize\nReset\n\n"
REQ_SIZE = 1448
SIZE = 0
LINES = 0
WAIT_TIME = 0.1
RTT = 0.01
N = 1
PACKETS = 0

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
    # print("Receiving size.")
    global SIZE,RTT
    while True:
        t = time.time()
        try:
            if reset:
                server.sendto(RESET_MESSAGE,(UDP_IP_OTHER, UDP_PORT_OTHER))
            else:
                server.sendto(MESSAGE,(UDP_IP_OTHER, UDP_PORT_OTHER))
            data = server.makefile("r", encoding="utf8", newline="\n")
            # raw,addr = server.recvfrom(REQ_SIZE)
            raw = data.readline()
            SIZE = int(raw.split(':')[1])
            # print(f"Received a the size of file : {SIZE}")
            RTT += (time.time()-t)
            # print(RTT)
            break
        except:
            # print("Size re quest not sent or packet was dropped.")
            pass
    # print("Successful received the size of file.")

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
    while True:
        try:
            server.sendto(msg.encode(),(UDP_IP_OTHER, UDP_PORT_OTHER))
            time.sleep(3*RTT)
            data,_=server.recvfrom(10000)
            datas = data.decode().split('\n')
            # print(datas)
            for line in datas:
                print(line)
                if line.startswith("Penalty"):
                    return
        except:
            pass
    print("Successful submitted.")

def flush(server:socket.socket) -> None:
    print("Flushing previous messages...")
    time.sleep(5*RTT)
    i = 0
    while True:
        try:
            data,_=server.recvfrom(2000)
            # print(data.decode())
        except:
            i += 1
            if i == 4:
                break

# Think about multiple requests coming together. 
# Receiving messages in parallel
def recv_msg(server:socket.socket,n:int) -> int:
    # print("Receiving messages...")
    global N,LINES,PACKETS,file_lines,RTT
    i = 0
    received = 0
    while i <= n:
        i += 1
        try:
            data,_ = server.recvfrom(2000)
            data = data.decode().split('\n',3)
            _,offset_ = data[0].split(": ")
            byte_to_string_stream = data[3]

            if data[2] == "Squished":
                print("Squished ------------------------")
                print("Squished ------------------------")
                print("Squished ------------------------")
                print("Squished ------------------------")
                print("Squished ------------------------")
                N = (N+1)//2
                file_lines[int(offset_)] = byte_to_string_stream[1:]
            else:
                file_lines[int(offset_)] = byte_to_string_stream

            # Deletion from the place we are requesting the offset
            if int(offset_) in ack_queue:
                ack_queue.pop(int(offset_))
            received += 1
        except:
            pass
    # print("Messages received.")
    return received

# Requesting messages in parallel
def req_msg(server:socket.socket) -> None:
    print("Requesting messages...")
    global N,PACKETS
    while True:
        if len(ack_queue) == 0:
            break
        # msg_to_be_requested = dict[int,int]()
        n = min(N,len(ack_queue))
        i = 0
        for offset,size in ack_queue.items():
            if (i == n): break
            server.sendto(msg_to_bytes(offset,size),(UDP_IP_OTHER, UDP_PORT_OTHER))
            i += 1
        time.sleep(RTT*(3))
        received =recv_msg(server,n)
        if n <= 5:
            if 10*received < n*7:
                N = (N+1)//2
            else:
                N += 1
        elif n <= 10:
            if 10*received < n*8:
                N = (N+1)//2
            else:
                N += 1
        else:
            if 10*received < n*9:
                N = (N+1)//2
            else:
                N += 1
    print("All message requested!")

t = time.time()
# Main block
with socket.socket(family=socket.AF_INET,type=socket.SOCK_DGRAM) as server:
    server.settimeout(WAIT_TIME)
    recv_size(server,True)
    for i in range(9):
        recv_size(server,False)                               # Get size
    RTT /= 10
    server.settimeout(RTT)
    initialize_queue(SIZE,REQ_SIZE)                 # Initialize DS
    req_msg(server=server)
    flush(server)
    submit(server)                                        # Perform submission
