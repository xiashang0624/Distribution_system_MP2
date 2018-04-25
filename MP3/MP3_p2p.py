import socket, time, threading
from random import randint

# Chord protocol is to achieve p2p application under changing peer groups
# There should be threads running all the time:
# 1. node_0.server().  node_0 initialed to hold all the keys, and every other nodes joined later
#    knows how to connect with node_0
# 2. client_input(), waiting for input from the keyboard
# 3. client_execute(), execute the command from buffer one by one
# 4. if node_n joined later,  node_n.server() should run all the time before it is crashed.

operation_buffer = []
'''
    ch_node represents a peer in the network
'''
class ch_node:
    identifier = -1
    pred_node = None
    succ_node = None
    table = []
    keys = []
    s = None

    def __init__(self, id, ini_keys):
        # default keys are null, except for initial node 0
        self.keys = ini_keys
        self.identifier = id
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.s.bind(addr_list[id])
        listen_thread = threading.Thread(target=self.server, args=(self.s,))
        listen_thread.start()

        # find pred_node and succ_node
        # key transferred from pred_node
        pass

    def find_key(self):
        pass

    def find_node(self):
        pass

    def server(self, socket):
        while True:
            try:
                recieve_msg, addr = socket.recvfrom(1024)
                receive_src = addr[-1] % 10
                receive_str = recieve_msg.decode('utf-8')
                msg = receive_str.split(',')
                operata = msg[0]
                # recv_time = time.asctime().strip().split()[3]
                if operata == 'find':
                    pass
                elif operata == 'show':
                    pass
                elif operata == 'crash':
                    pass
            except:
                pass
        pass


def client_input():
    while True:
        message = input('input your command:\n')
        msg = message.split()
        if msg[0] == 'exit':
            print('manually exit ')
            exit()
        elif msg[0] in {'crash',  'join', 'show'}:
            operation_buffer.append(msg)
        else:
            print('Invalid input')


def client_execute(s):
    input_thread = threading.Thread(target=(client_input()), args=(s))
    input_thread.start()
    while True:
        if len(operation_buffer) != 0:
            msg = operation_buffer.pop(0)
            if msg[0] == 'crash':
                Unicast(s, msg[1], msg[0])
            elif msg[0] == 'show':
                if msg[1] == 'all':
                    for i in range(32):
                        Unicast(s,i,msg[0])
                # send show command to all nodes
                else:
                    Unicast(s,i,msg[0])
                # send show command to node p
            elif msg[0] == 'join':
                pass
                # create a new peer
        time.sleep(0.05)

    # show current time
    # req_time = time.asctime().split()[3].replace(':', '')
    # print('dump ' + req_time)


# ****************broadcast with delay***************
# define a function to uni-cast
def Unicast(client_socket, target, message):
    send_thread = threading.Thread(target = Delay, args = (client_socket, target, message,))
    send_thread.start()


# implement the delay mechanism
def Delay(client_socket, target, message):
    delay_time = randint(min_delay, max_delay)/1000.0
    # set it to 0 to remove the delay mechanism
    time.sleep(delay_time)
    client_socket.sendto(message.encode('utf-8'), addr_list[target])


# Unordered multi-cast
def Multicast_unorder(client_socket, message):
        for i in range(number_replica):
            Unicast(client_socket, i, message)


# **********Main**************
# get the process IP and port info based on the selected number
def process_info(number):
    address = port_info[number][1]
    port = int(port_info[number][2])
    return address, port


# read the config file
with open('config.txt') as f:
    content = f.readlines()

# save the min_delay and max_delay in two varibles
min_delay, max_delay = content[0].strip().split()
min_delay, max_delay = int(min_delay), int(max_delay)

# save the other information in the port_info list
port_info = []
for i in content[1:-1]:
    port_info.append(i.strip().split())

addr_list = []
for i in range(33):
    addr_list.append(process_info(i))


# # initialize a log file
# file_name = 'log' + str(process_number) + '.txt'
# # write log file
# def write_to_file(name, text):
#     log_file = open(name, "a+")
#     log_file.write(text+'\r\n')
#     log_file.close()


all_keys = []
for i in range(255):
    all_keys.append(i)
ini_node = ch_node(0, all_keys)


# bind socket to the ip address based on the config file
client_socket= socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_socket.bind(addr_list[-1])
client_execute(client_socket)