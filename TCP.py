import socket,hashlib,time

class TCP:
    def __init__(self,ip_connect,port_connect) -> None:
        # Server
        self.UDP_IP_OTHER = ip_connect
        self.UDP_PORT_OTHER = port_connect

        # Protocols
        self.MESSAGE = b"SendSize\n\n"
        self.RESET_MESSAGE = b"SendSize\nReset\n\n"
        self.REQ_SIZE = 1448
        self.SIZE = 0
        self.LINES = 0
        self.WAIT_TIME = 0.1
        self.RTT = 0.007

        self.RTT_array = []
        self.N = 5
        self.squished = 0
        # Offset queue
        self.ack_queue = dict[int,int]()
        # File
        self.file_lines = dict[int,str]()
        self.send_time = dict()

    # Message to bytes
    def msg_to_bytes(self,offset: int,byte: int) -> bytes:
        temp: str = f"Offset: {offset}\nNumBytes: {byte}\n\n"
        return temp.encode()
    
    def recv_size(self,reset : bool = False) -> None:
        while True:
            t = time.time()
            try:
                if reset:
                    self.server.sendto(self.RESET_MESSAGE,(self.UDP_IP_OTHER, self.UDP_PORT_OTHER))
                else:
                    self.server.sendto(self.MESSAGE,(self.UDP_IP_OTHER, self.UDP_PORT_OTHER))
                data = self.server.makefile("r", encoding="utf8", newline="\n")
                # raw,addr = server.recvfrom(REQ_SIZE)
                raw = data.readline()
                self.SIZE = int(raw.split(':')[1])
                # print(f"Received a the size of file : {SIZE}")
                self.RTT_array.append(time.time()-t)
                # print(RTT)
                break
            except:
                # print("Size re quest not sent or packet was dropped.")
                pass
        # print("Successful received the size of file.")

    # Queue of block not received
    def initialize_queue(self,size:int,packet_size:int) -> None:
        print("Initializing queue.")
        self.LINES, self.squished = 0,0
        self.ack_queue.clear()
        self.file_lines.clear()
        cnt = 0
        increment:int = 0
        while (increment < size):
            self.ack_queue[increment] = packet_size
            increment += packet_size
            self.LINES += 1
        increment -= packet_size
        self.ack_queue[increment] = size-increment
        print("Queue initialized!")

    # Hash
    def MD5_Hash(self) -> str:
        print("Generating hash.")
        req_size:int = 0
        byte_stream:str = ""
        while req_size < self.SIZE:
            byte_stream += self.file_lines[req_size]
            req_size += self.REQ_SIZE
        byte_stream_:str = hashlib.md5(byte_stream.encode()).hexdigest()
        print("Hash generated!")
        print("Hash ->",byte_stream_)
        return byte_stream_

    # Submission protocol
    def submit(self) -> None:
        print("Submitting messages...")
        data_ = self.MD5_Hash()
        msg = f"Submit: Mayank@Mayank\nMD5: {data_}\n\n"
        last = ""
        last_last = ""
        while True:
            try:
                self.server.sendto(msg.encode(),(self.UDP_IP_OTHER, self.UDP_PORT_OTHER))
                time.sleep(2*self.RTT)
                data,_=self.server.recvfrom(10000)
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
    def flush(self) -> None:
        print("Flushing previous messages...")
        time.sleep(5*self.RTT)
        i = 0
        while True:
            try:
                data,_=self.server.recvfrom(2000)
            except:
                i += 1
                if i == 4:
                    break
        print("Previous messages flushed!")

    # Receiving messages
    def recv_msg(self,n:int) -> int:
        i = 0
        received = 0
        while i <= n:
            i += 1
            try:
                data,_ = self.server.recvfrom(2000)
                data = data.decode().split('\n',3)
                _,offset_ = data[0].split(": ")
                byte_to_string_stream = data[3]
                if (len(data) != 4):
                    continue
                if data[2] == "Squished":
                    self.squished += 1
                    print("Squished :(")
                    self.RTT *= (1.01)
                    self.server.settimeout(self.RTT)
                    self.N = (self.N+1)//2
                    self.file_lines[int(offset_)] = byte_to_string_stream[1:]
                else:
                    self.file_lines[int(offset_)] = byte_to_string_stream
                    if self.UDP_IP_OTHER != '':
                        self.RTT = 0.8*self.RTT + 0.2*(time.time()-self.send_time[int(offset_)])
                        self.server.settimeout(self.RTT)
                if int(offset_) in self.ack_queue:
                    self.ack_queue.pop(int(offset_))
                    received += 1
            except:
                if self.UDP_IP_OTHER != '':
                    self.RTT *= 1.005
        return received

    # Requesting messages
    def req_msg_aimd(self) -> None:
        print("Requesting messages...")
        while True:
            print(len(self.ack_queue),self.RTT)
            if len(self.ack_queue) == 0:
                break
            n = min(self.N,len(self.ack_queue))
            i = 0
            for offset,size in self.ack_queue.items():
                if (i == n): break
                self.send_time[offset] = time.time()
                self.server.sendto(self.msg_to_bytes(offset,size),(self.UDP_IP_OTHER,self.UDP_PORT_OTHER))
                i += 1
            received =self.recv_msg(n+3)
            if received < n:
                self.N = (self.N+1)//2
            else:
                self.N += 1
        print("All message requested!")

    def req_msg_aiad(self) ->None:
        print("Requesting messages...")
        while True:
            print(len(self.ack_queue),self.RTT)
            if len(self.ack_queue) == 0:
                break
            n = min(self.N,len(self.ack_queue))
            i = 0
            for offset,size in self.ack_queue.items():
                if (i == n): break
                self.send_time[offset] = time.time()
                self.server.sendto(self.msg_to_bytes(offset,size),(self.UDP_IP_OTHER,self.UDP_PORT_OTHER))
                i += 1
            received =self.recv_msg(n+3)
            if received < n:
                self.N -= 1
            else:
                self.N += 1
        print("All message requested!")
    
    def start(self, n:int = 1,mode=0):
        with socket.socket(family=socket.AF_INET,type=socket.SOCK_DGRAM) as self.server:
            self.server.settimeout(self.WAIT_TIME)
            t = time.time()
            self.recv_size(True)
            for i in range(99):
                self.recv_size(False)
            self.RTT_array.sort()
            mid = len(self.RTT_array)//2
            self.RTT = max((self.RTT_array[mid]+self.RTT_array[-mid])/2, self.RTT)       # Median of 100 samples of RTT
            self.server.settimeout(self.RTT)
            self.initialize_queue(self.SIZE,self.REQ_SIZE)                 # Initialize DS
            if (mode == 0): self.req_msg_aiad()
            else: self.req_msg_aimd()                          # Request messages
            self.flush()                                   # Flushing buffer
            self.submit()                                  # Perform submission
            print("Squishes:", self.squished//100 + (self.squished%100 > 0))
            self.RTT_array.clear()

server = TCP('10.17.7.218',9802)
server.start(1)
