import socket, time, threading
from random import randint
import pdb

# Chord protocol is to achieve p2p application under changing peer groups
# There should be threads running all the time:
# 1. node_0.server().  node_0 initialed to hold all the keys, and every other nodes joined later
#    knows how to connect with node_0
# 2. client_input(), waiting for input from the keyboard
# 3. client_execute(), execute the command from buffer one by one
# 4. if node_n joined later,  node_n.server() should run all the time before it is crashed.

Command_buff = []
receive_buffer = []
'''
    ch_node represents a peer in the network
'''

global flag
# *************** Listen and responding ****************
# following part is replica_server to listen any operations for modifying the replica
def Node_listen(node_socket):
    while True:
        try:
            recieve_msg,addr = node_socket.recvfrom(1024)
            receive_src = addr[-1] % 100
            receive_str = recieve_msg.decode('utf-8')
            msg = receive_str.split()
            operata = msg[0]
            # recv_time = time.asctime().strip().split()[3]
            if operata == 'find_response':
                receive_buffer.append(receive_str)

            if operata == 'find_predecessor':
                msg = 'find_predecessor ' + str(id) + ' ' + str(P_ID) + ' ' + addr_lookup[P_ID]
                find_response(int(msg[1]), int(msg[2]), msg[3])
        except:
            pass


def find_response(id, request_node_id, request_node_addr):
    dest_id, dest_addr = find_predecessor(id, request_node_id, request_node_addr)
    if dest_id == P_ID:
        msg = 'find_response' + str(dest_id)+' ' + str(dest_addr)
        Unicast(s, request_node_addr, msg)


# ******************* Client ****************
# Client input: depending on the input format,
# delay function embedded.
def Client_input():
    while True:
        message = input('Client: enter command here:\n')
        msg = message.split()
        if not msg:
            print('Error input, input should use the following format: find/join/crash + node + (key)')
            pass
        else:
            command= msg[0]
            if command == 'find' and len(msg)==3: # write the get command to log file and broadcast
                print ('find command issued: ' + message)
            else:
                print('Error input, input should use the following format: put/get/delay/dump + (key + value).')
                pass


def client_execute(s):
    input_thread = threading.Thread(target=Client_input, args=(s))
    input_thread.start()
    while True:
        if len(Command_buff) != 0:
            msg = Command_buff.pop(0)
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
    client_socket.sendto(message.encode('utf-8'), target)


# ***************Chrod functions ********************************
# find the succ and pred
def find_successor(id):
    n_id, n_addr = find_predecessor(id, P_ID, addr_lookup[P_ID])
    return n_id, n_addr


def find_predecessor(id, src_id, src_addr):
    n_id, n_addr= P_ID, addr_list[P_ID]
    if not (n_id < id and n_id >= FT_succ[0]):
        # to verify if it is between two consecutive node
        n_id, n_addr = closest_preceding_finger(id)
        msg = 'find_predecessor '+str(id) + ' ' + str(src_id) + ' ' + src_addr
        Unicast(s, n_addr, msg)
        flag = True
        while(flag):
            time.sleep(0.05)
        n_id, n_addr = receive_buffer.pop()
    if src_id == P_ID:
        return n_id, n_addr
    else:
        response = 'find_response ' + str(n_id) + ' ' + str(n_addr)
        Unicast(s, src_addr, response)


def closest_preceding_finger(id):
    for i in range(7,-1):
        if FT_succ[i] > P_ID and FT_succ[i] < id:
            # gradually get close to the closest point
            return FT_succ[i], addr_lookup[FT_succ[i]]
    return P_ID, addr_lookup[P_ID]


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

# Initialize the process information: process number, host address, and IP
# address

P_ID= 9999
PID_pool = set()
for i in range(32):
    PID_pool.add(i)

while P_ID not in PID_pool:
    P_ID= input('Select the process number from 0-31:' )
    P_ID= int(P_ID)

print('The process number selected is: {}'.format(P_ID))

# Bind node to a socket and use one thread to listen messages
identifier = P_ID
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(addr_list[P_ID])
listen_thread = threading.Thread(target=Node_listen, args=(s,))
listen_thread.start()


# Initilize finger table and Keys
FT_start = []
FT_succ = []
keys = []

FT_interval = [] # function as 'mod' to finger table

addr_lookup = {}
addr_lookup[0] = addr_list[0]
addr_lookup[1] = addr_list[1]
addr_lookup[100] = addr_list[2]
addr_lookup[200] = addr_list[3]

if P_ID == 0:
    for i in range(8):
        FT_start.append(P_ID + 2**i)
    Node_ID = 0
    FT_succ=[1,100,100,100,100,100,100,200]
    keys.append(0)
    for i in range(201,256):
        keys.append(i)

elif P_ID ==1:
    for i in range(8):
        FT_start.append(P_ID + 2**i)
    Node_ID = 1
    FT_succ=[100,100,100,100,100,100,100,200]
    keys.append(1)

elif P_ID ==2:
    for i in range(8):
        FT_start.append(100 + 2**i)
    Node_ID = 100
    FT_succ=[200,200,200,200,200,200,200,0]
    for i in range(2,101):
        keys.append(i)

elif P_ID ==3:
    for i in range(8):
        FT_start.append((200 + 2**i)%2**8)
    Node_ID = 200
    FT_succ=[0,0,0,0,0,0,100,100]
    for i in range(101,201):
        keys.append(i)


print ('\nFT_start for P_ID %2d:'%Node_ID)
print (FT_start)
print ('\nFT_succ for P_ID %2d:'%Node_ID)
print (FT_succ)
print ('\nKeys for P_ID %2d:\n'%Node_ID)
print (keys)



# the main program is used for clinet to take input message and process it
Client_input()

