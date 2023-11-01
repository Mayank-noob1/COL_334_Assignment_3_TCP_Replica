import socket,hashlib,time

# Server
UDP_IP_OTHER = '10.17.7.134'
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
RTT = 0.007

RTT_array = []
N = 5
squished = 0
# Offset queue
ack_queue = dict[int,int]()
# File
file_lines = dict[int,str]()
send_time = dict()

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
            RTT_array.append(time.time()-t)
            # print(RTT)
            break
        except:
            # print("Size re quest not sent or packet was dropped.")
            pass
    # print("Successful received the size of file.")

# Queue of block not received
def initialize_queue(size:int,packet_size:int) -> None:
    print("Initializing queue.")
    global ack_queue,LINES,squished
    LINES, squished = 0,0
    ack_queue.clear()
    file_lines.clear()
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
    last = ""
    last_last = ""
    while True:
        try:
            server.sendto(msg.encode(),(UDP_IP_OTHER, UDP_PORT_OTHER))
            time.sleep(2*RTT)
            data,_=server.recvfrom(10000)
            datas = data.decode().split('\n')
            for line in datas:
                if line.startswith("Penalty"):
                    if ("Result: true"  ==  last_last[-12:]):
                        print("Result: true")
                    else:
                        print("Result: false")
                    print(last)
                    print(line)
                    print("Successful submitted.")
                    return
                last_last = last
                last = line
        except:
            pass

# Flushes the buffer
def flush(server:socket.socket) -> None:
    print("Flushing previous messages...")
    time.sleep(5*RTT)
    i = 0
    while True:
        try:
            data,_=server.recvfrom(2000)
        except:
            i += 1
            if i == 4:
                break
    print("Previous messages flushed!")

# Receiving messages
def recv_msg(server:socket.socket,n:int) -> int:
    global N,LINES,PACKETS,file_lines,RTT, squished,send_time
    i = 0
    received = 0
    while i <= n:
        i += 1
        try:
            data,_ = server.recvfrom(2000)
            data = data.decode().split('\n',3)
            _,offset_ = data[0].split(": ")
            byte_to_string_stream = data[3]
            if (len(data) != 4):
                continue
            if data[2] == "Squished":
                squished += 1
                print("Squished :(")
                RTT *= (1.01)
                server.settimeout(RTT)
                N = (N+1)//2
                file_lines[int(offset_)] = byte_to_string_stream[1:]
            else:
                file_lines[int(offset_)] = byte_to_string_stream
                if int(offset_) in ack_queue:
                    ack_queue.pop(int(offset_))
                    if UDP_IP_OTHER != '':
                        RTT = 0.8*RTT + 0.2*(time.time()-send_time[int(offset_)])
                        server.settimeout(RTT)
            # Deletion from the place we are requesting the offset
            received += 1
        except:
            if UDP_IP_OTHER != '':
                RTT *= 1.001
    return received

# Requesting messages
def req_msg(server:socket.socket) -> None:
    print("Requesting messages...")
    global N,RTT
    while True:
        print(len(ack_queue),RTT)
        if len(ack_queue) == 0:
            break
        n = min(N,len(ack_queue))
        i = 0
        for offset,size in ack_queue.items():
            if (i == n): break
            send_time[offset] = time.time()
            server.sendto(msg_to_bytes(offset,size),(UDP_IP_OTHER, UDP_PORT_OTHER))
            i += 1
        received =recv_msg(server,n+2)
        if received < n:
            N = (N+1)//2
        else:
            N += 1
    print("All message requested!")

# Main flow
for _ in range(1):
    with socket.socket(family=socket.AF_INET,type=socket.SOCK_DGRAM) as server:
        server.settimeout(WAIT_TIME)
        t = time.time()
        recv_size(server,True)
        for i in range(99):
            recv_size(server,False)
        RTT_array.sort()
        mid = len(RTT_array)//2
        RTT = max((RTT_array[mid]+RTT_array[-mid])/2, RTT)       # Median of 100 samples of RTT
        server.settimeout(RTT)
        initialize_queue(SIZE,REQ_SIZE)                 # Initialize DS
        req_msg(server=server)                          # Request messages
        flush(server)                                   # Flushing buffer
        submit(server)                                  # Perform submission
        print("Squishes:", squished//100 + (squished%100 > 0))
        RTT_array.clear()
