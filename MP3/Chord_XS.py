import socket, time, threading
from random import randint
import pdb

# Chord protocol is to achieve p2p application under changing peer groups
# There should be threads running all the time:
# 1. node_0.server().  node_0 initialed to hold all the keys, and every other nodes joined later
#    knows how to connect with node_0
# 2. client_input(), waiting for input from the keyboard

# *************** Listen and responding ****************
# following part is replica_server to listen any operations for modifying the replica
def Node_listen(node_socket):
    global wait_flag
    global find_pred_flag
    global ip_pair
    global recv_nn_id, recv_nn_succ

    while True:
        try:
            recieve_msg,addr = node_socket.recvfrom(1024)
            receive_str = recieve_msg.decode('utf-8')
            msg = receive_str.split()
            print (msg)
            command = msg[0]
            if command == 'find_pred_routing':
                id, root = int(msg[2]), int(msg[4])
                find_predecessor_routing(id, root)

            elif command == 'pred_and_succ_found':
                recv_nn_id, recv_nn_succ = int(msg[1]), int(msg[2])
                find_pred_flag = False


            elif command == 'initialize':
                # here is to process message with command "initialize id
                # succ_node" in node join operation
                print ('start node_initialize')
                print (msg)
                id, succ_node_str = int(msg[1]), msg[2:]
                init_thread = threading.Thread(target = node_initialize, args=(id, succ_node_str,))
                init_thread.start()

            elif command == 'node_join_done':
                wait_flag = False

            elif command == 'add_ip_pair':
                ip_pair[int(msg[1])] = int(msg[2])
                print ('update ip_pair')
                print (ip_pair)

            elif command == 'update_finger_table':
                s0, i0 = int(msg[1]), int(msg[2])
                print ("start update_finger_table")
                update_finger_table(s0, i0, receive_str)

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
    global wait_flag
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
                while wait_flag:
                    time.sleep(0.1)
                print ("node join has been done!")
                print ('The pred node is: %2d and the succ node is: %2d' % (target_node, succ_node))
            elif command == 'find' and len(msg)==3: # write the get command to log file and broadcast
                print ('find command issued: ' + message)
            elif command == 'FT_succ':
                print (FT_succ)
            elif command == 'FT_start':
                print (FT_start)
            else:
                print('Error input, input should use the following format: put/get/delay/dump + (key + value).')
                pass


# ****************unicast with delay***************
# define a function to uni-cast
def Unicast(client_socket, target, message, No_delay = False):
    send_thread = threading.Thread(target = Delay, args = (client_socket, target, message, No_delay,))
    send_thread.start()


# implement the delay mechanism
def Delay(client_socket, target, message, No_delay = False):
    delay_time = randint(min_delay, max_delay)/1000.0
    # set it to 0 to remove the delay mechanism
    if No_delay:
        delay_time = 0
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
    global find_pred_flag
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
        message = 'find_pred_routing ' + str(nn_id) +' ' + str(id) + ' from ' + str(node_ID)
        find_pred_flag = True
        Unicast(s, addr_list[ip_pair[nn_id]], message)
        while find_pred_flag:
            time.sleep(0.1)
        print ("find predecessor lock is released!!!")
        return recv_nn_id, recv_nn_succ


def find_predecessor_routing(id, root_id):
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
        message = 'pred_and_succ_found '+ str(nn_id) + ' ' + str(nn_succ)
        print (ip_pair[root_id])
        Unicast(s, addr_list[ip_pair[root_id]], message)
    else:
        # look through the finger table at node nn_id
        nn_id = closest_preceding_finger(id)
        message = 'find_pred_routing ' + str(nn_id) +' ' + str(id) + ' from ' + str(root_id)
        Unicast(s, addr_list[ip_pair[nn_id]], message)


def closest_preceding_finger(id):
    for i in range(7, -1, -1):
        if id < node_ID:
            cond = FT_succ[i] > node_ID or FT_succ[i] < id
        else:
            cond = FT_succ[i] > node_ID and FT_succ[i] < id
        if cond:
            return FT_succ[i]
    return node_ID


def join_node(id):
    global ip_pair
    global wait_flag
    nn_pre, nn_succ = find_successor(id)
    # assign new ip address to the new node id and update ip_pair
    new_pid = len(ip_pair)
    ip_pair[id] = new_pid
    # here I simply use multicast to update the ip-pair variable across all
    # nodes.
    message = 'add_ip_pair ' + str(id) + ' ' + str(new_pid)
    for i in range (1,32):
        Unicast(s, addr_list[i], message, No_delay = True)

    # update figer table for new node
    new_FT_start = []
    new_FT_succ = []
    for i in range(8):
        new_FT_start.append((id + 2**i)%(2**8))
    new_FT_succ.append(nn_succ)

    for i in range(7):
        if new_FT_succ[i] < id:
            cond = new_FT_start[i+1] >= id or new_FT_start[i+1] < new_FT_succ[i]
        elif new_FT_succ[i] > id:
            cond = new_FT_start[i+1] >= id and new_FT_start[i+1] < new_FT_succ[i]
        else:
            cond = True
        print (cond)
        if cond:
            new_FT_succ.append(new_FT_succ[i])
        else:
            new_pre, new_succ = find_successor(new_FT_start[i+1])
            if (new_succ-new_FT_start[i+1])%2**8 > (id-new_FT_start[i+1])%2**8 or new_pre == new_succ:
                new_FT_succ.append(id)
            else:
                new_FT_succ.append(new_succ)
    print (new_FT_succ)
    print (new_FT_start)

    node_str = ''
    for i in new_FT_succ:
        node_str += ' ' + str(i)
    message = 'initialize ' + str(id) + node_str
    wait_flag = True
    print ("start initiating new node %3d for process %2d" % (id, new_pid))
    Unicast(s, addr_list[new_pid], message)
    return nn_pre, nn_succ


def node_initialize(id, node_str):
    # initialize the node after joining into the Chord system using the sudo
    # code from the original paper
    global FT_start
    global FT_succ
    global Keys
    global node_ID
    print ("start initializa")
    node_ID = id
    for i in range(8):
        FT_start.append((node_ID + 2**i)%(2**8))
        FT_succ.append(int(node_str[i]))
    print (node_ID)
    print ("node initialization is done!!!")
    print ("FT_start is:")
    print (FT_start)
    print ("FT_succ is:")
    print (FT_succ)

    # update the finger tables in the other nodes
    Update_others(node_ID)


    Unicast(s, addr_list[0], 'node_join_done')

def Update_others(id):
    for i in range(8):
        print ("update start:")
        print (i)
        p,p_succ = find_predecessor((id - 2**i) % 2**8)
        print ("check alskdjflaskdjf")
        print (p)
        if p != node_ID:
            message = 'update_finger_table ' + str(id) + ' ' + str(i)
            Unicast(s, addr_list[ip_pair[p]], message)

def update_finger_table(s0, i0, message):
    global FT_succ
    if FT_succ[i0] < node_ID:
        cond = s0 >= node_ID or s0 < FT_succ[i0]
    elif FT_succ[i0] > node_ID:
        cond = s0 >= node_ID and s0 <FT_succ[i0]
    else:
        cond = True
    if cond:
        FT_succ[i0] = s0
        print ("change FT_succ of i = %2d to node %2d"%(i0, s0))
        pred_node = node_pred(node_ID)
        print ("pred_node of node id %2d is: %2d "%(node_ID, pred_node))
        if pred_node != node_ID and pred_node!=s0:
            Unicast(s, addr_list[ip_pair[pred_node]], message)


def node_pred(node):
    pred = node
    distance = 256
    for key, value in ip_pair.items():
        new_distance = (node - key) % 2**8
        if  new_distance < distance and key != node:
            pred, distance = key, new_distance

    return pred



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

ip_pair[0] = 0 # here we use the 0 process to bind node id 0

if P_ID == 0:
    node_ID = 0
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
Client_input()

