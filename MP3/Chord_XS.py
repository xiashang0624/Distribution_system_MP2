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
            receive_str = recieve_msg.decode('utf-8')
            msg = receive_str.split()
            command = msg[0]
            if command == 'find_pred_routing':
                receive_buffer.append(receive_str)

            if command == 'initialize':
                # here is to process message with command "initialize id
                # succ_node" in node join operation
                print (msg)
                id, succ_node = int(msg[1]), int(msg[2])
                node_initialize(id, succ_node)
                print ("join operation is finished")
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
            if command == 'join' and len(msg)==2: # write the get command to log file and broadcast
                print ('join command issued: ' + message)
                id = int(msg[1])
                target_node, succ_node = join_node(id)
                print ('The pred node is: %2d and the succ node is: %2d' % (target_node, succ_node))
            elif command == 'find' and len(msg)==3: # write the get command to log file and broadcast
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
    print ("join message has been sent via socket!!!")
    print (message)
    print (target)


# ***************Chrod functions ********************************
# find the succ and pred
def find_successor(id):
    nn_pre, nn_succ= find_predecessor(id)
    return nn_pre, nn_succ


def find_predecessor(id):
    global wait_flag
    global recv_nn_id, recv_nn_succ
    nn_id, nn_succ= node_ID, FT_succ[0]
    # check if id is in the range between nn_id and nn_succ
    if nn_succ < nn_id:
        cond = id > nn_id or id <= nn_succ
    elif nn_succ > nn_id:
        cond = id > nn_id and id <= nn_succ
    else:
        cond = True

    if cond:
        print ('predecessor for id: %2d is found. Node id is: %2d '
               % (id, node_ID))
        return nn_id, nn_succ
    else:
        # look through the finger table at node nn_id
        nn_id = closest_preceding_finger(id)
        message = 'find_predecessor ' + str(nn_id) +' ' + str(id) + ' from ' + str(node_ID)
        wait_flag = True
        Unicast(s, addr_list[ip_pair[node_ID]], message)
        while wait_flag:
            time.sleep(0.1)
        return recv_nn_id, recv_nn_succ


def find_predecessor_routing(id):
    nn_id, nn_succ= node_ID, FT_succ[0]
    # check if id is in the range between nn_id and nn_succ
    if nn_succ < nn_id:
        cond = id > nn_id or id <= nn_succ
    elif nn_succ > nn_id:
        cond = id > nn_id and id <= nn_succ
    else:
        cond = True

    if cond:
        print ('predecessor for id: %2d is found. Node id is: %2d '
               % (id, node_ID))
        message = 'pred_and_succ found: '+ str(nn_id) + ' ' + 'nn_succ'
        Unicast(s, addr_list[0], message)
    else:
        # look through the finger table at node nn_id
        nn_id = closest_preceding_finger(id)
        message = 'find_predecessor ' + str(nn_id) +' ' + str(id) + ' from ' + str(node_ID)
        Unicast(s, addr_list[ip_pair[node_ID]], message)


def closest_preceding_finger(id):
    for i in range(7, -1):
        if id < node_ID:
            cond = FT_succ[i] > node_ID or FT_succ[i] < id
        else:
            cond = FT_succ[i] > node_ID and FT_succ[i] < id
        if cond:
            return FT_succ[i]
    return node_ID


def join_node(id):
    global ip_pair
    nn_pre, nn_succ = find_successor(id)
    message = 'initialize ' + str(id) + ' ' + str(nn_succ)
    # assign new ip address to the new node id and update ip_pair
    new_pid = len(ip_pair)
    ip_pair[id] = new_pid
    Unicast(s, addr_list[new_pid], message)

    return nn_pre, nn_succ


def node_initialize(id, succ_node):
    # initialize the node after joining into the Chord system using the sudo
    # code from the original paper
    global FT_start
    global FT_succ
    global Keys
    global node_ID
    node_ID = id
    for i in range(8):
        FT_start.append((node_ID + 2**i)%(2**8))
    FT_succ.append(succ_node)
    print (node_ID)
    print (FT_start)
    print (FT_succ)
    for i in range(7):
        if FT_succ[i] < node_ID:
            cond = FT_start[i+1] >= node_ID or FT_start[i+1] < FT_succ[i]
        elif FT_succ[i] > node_ID:
            cond = FT_start[i+1] >= node_ID and FT_start[i+1] < FT_succ[i]
        else:
            cond = True
        print (cond)
        if cond:
            FT_succ.append(FT_succ[i])
        else:
            new_pre, new_succ = find_successor(FT_start[i+1])
            FT_succ.append(new_succ)

    print ("node initialization is done!!!")
    print ("FT_start is:")
    print (FT_start)
    print ("FT_succ is:")
    print (FT_succ)

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


# define some global variables in each node
# Initilize finger table and Keys
FT_start = []
FT_succ = []
Keys = []
ip_pair = {} # a dictionary to save the node_ID-P_ID pairs
FT_interval = [] # function as 'mod' to finger table

if P_ID == 0:
    node_ID = 0
    ip_pair[node_ID] = P_ID
    for i in range(8):
        FT_start.append(node_ID + 2**i)
    FT_succ=[0,0,0,0,0,0,0,0]
    for i in range(1,256):
        Keys.append(i)
    Keys.append(0)


wait_flag = False # wait flag to ensure only one message passing
recv_nn_id, recv_nn_succ = 0,0  # update the target node id and succ of the node id


print (FT_start)
print (Keys)
# the main program is used for clinet to take input message and process it
if P_ID== 0:
    Client_input()

