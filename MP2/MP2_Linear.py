import socket, time, threading, queue
from random import randint
import pdb

# Share-memory consistency with linearizability.
# The third algorithm discussed in lecture is used to impletment the
# linear-shared memory consistency model.  Total order is used for both read and
# write command

# Total order concept: one process is used as the leader to put a marker infront
# of the multicast request message.

# Each process also has a buffer (msg_memory), which is a dictionary in this
# implementation. Once the message is delievered, the msg will be removed from
# the buffer.


# define a function to listen for the leader in total cast
def total_listen_leader(server_socket):
    global msg_memory
    mark = 0
    while True:
        try:
            recieve_msg,addr = server_socket.recvfrom(1024)
            decode_msg = recieve_msg.decode('utf-8')
            if decode_msg.split()[0]!='Marker':
                # Here I use the following format to assign a numeric marker to
                # each message recived using with the leader: "*Marker number :"
                marked_msg = 'Marker ' + str(mark) + ' :' + str(decode_msg)
                mark += 1
                cast_thread = threading.Thread(target = Multicast_from_leader, args=(server_socket, marked_msg))
                cast_thread.start()
            else:
                # save the message with time stamp in the buffer (msg_memory)
                msg_index_s = decode_msg.find(':') # find the starting index based on the symbol of colon
                recieved_marker = int(decode_msg.split()[1]) # get the index of marker in the msg
                msg_to_buffer = {recieved_marker: decode_msg[msg_index_s+1:]}
                msg_memory.update(msg_to_buffer)
        except:
            time.sleep(0.05)
            pass


# Total order leader broadcast msg,
def Multicast_from_leader(client_socket, message, leader_ID=0):
    for i in range(7):
        client_socket.sendto(message.encode('utf-8'),addr_list[i])


# define a function to listen for the other processes in total cast
def total_listen_other(server_socket):
    global msg_memory
    while True:
        try:
            recieve_msg,addr = server_socket.recvfrom(1024)
            decode_msg = recieve_msg.decode('utf-8')
            # save the message with time stamp in the buffer (msg_memory)
            msg_index_s = decode_msg.find(':') # find the starting index based on the symbol of colon
            recieved_marker = int(decode_msg.split()[1]) # get the index of marker in the msg
            msg_to_buffer = {recieved_marker: decode_msg[msg_index_s+1:]}
            msg_memory.update(msg_to_buffer)
        except:
            time.sleep(0.05)
            pass


# msg_memory value format: "P_ID Commmand Key Value".
# For example: "1 get x" means process 1 initilze command of read x.
# "2 put x 9" means process 2 initilize command of write x with value of 9
def Msg_deliver():
    global invoke
    mark_deliver = 0
    global msg_memory
    while True:
        if msg_memory and min(msg_memory) == mark_deliver:
            buff_msg = msg_memory[mark_deliver].split()
            if buff_msg[1] == 'get':
                if int(buff_msg[0]) == P_ID:
                    recv_time = int(time.time()*1000)  # get the timestampt in millisecond
                    text_to_file = '555,'+str(P_ID)+',get,'+buff_msg[-1]+','+str(recv_time)+','+'resp,'+str(share_V[buff_msg[-1]])
                    write_to_file(file_name, text_to_file)
                    invoke = True
            elif buff_msg[1] == 'put':
                if int(buff_msg[0]) == P_ID:
                    share_V[buff_msg[-2]] = int(buff_msg[-1])
                    recv_time = int(time.time()*1000)  # get the timestampt in millisecond
                    text_to_file = '555,'+str(P_ID)+',put,'+buff_msg[-2]+','+str(recv_time)+','+'resp,'+str(share_V[buff_msg[-2]])
                    write_to_file(file_name, text_to_file)
                    invoke = True
                else:
                    share_V[buff_msg[-2]] = int(buff_msg[-1])
            del msg_memory[mark_deliver]
            mark_deliver += 1
        time.sleep(0.01)


# implement the delay mechanism for the get request and send request to the client server
def Send_Delay(client_socket, target, message):
    delay_time = randint(min_delay, max_delay)/1000.0
    time.sleep(delay_time)
    send_message = str(P_ID) + ' ' + message    # here I put the process ID infront of the message to the leader
    client_socket.sendto(send_message.encode('utf-8'), addr_list[target])


# Client input: depending on the input format, initilize total order-multicast with a
# delay function embedded.
def Total_order_send_to_leader():
    global Command_buff
    while True:
        message = input('enter command here:')
        msg = message.split()
        if not msg:
            print('Error input, input should use the following format: put/get/delay/dump + key')
            pass
        else:
            command= msg[0]
            if command == 'get' and len(msg)==2 and msg[1] in share_V: # write the get command to log file and broadcast
                Command_buff.put(message)
            elif command == 'put' and len(msg) == 3 and msg[1] in share_V:          # write the put command to log file and broadcast
                Command_buff.put(message)
            elif command == 'delay' and len(msg) == 2:        # put the stdin sleep
                Command_buff.put(message)
            elif command == 'dump' and len(msg) == 1:    # print all the key-value pairs in shared memory
                Command_buff.put(message)
            else:
                print('Error input, input should use the following format: put/get/delay/dump + (key + value).')
                pass

def client_to_file(client_socket, leader=0):
    global Command_buff
    global invoke
    while True:
        if invoke == True and not Command_buff.empty():
            message= Command_buff.get()
            msg = message.split()
            send_time = int(time.time()*1000)  # get the timestampt in millisecond
            command= msg[0]
            if command == 'get' and len(msg)==2 and msg[1] in share_V: # write the get command to log file and broadcast
                key = msg[1]
                text_to_file = '555,'+str(P_ID)+','+command+','+key+','+str(send_time)+','+'req,'
                write_to_file(file_name, text_to_file)
                Send_Delay(client_socket, leader, message)
                invoke = False
            elif command == 'put' and len(msg) == 3 and msg[1] in share_V:          # write the put command to log file and broadcast
                key,value = msg[1], msg[2]
                text_to_file = '555,'+str(P_ID)+','+command+','+key+','+str(send_time)+','+'req'+','+str(value)+','
                write_to_file(file_name, text_to_file)
                Send_Delay(client_socket, leader, message)
                invoke = False
            elif command == 'delay' and len(msg) == 2:        # put the stdin sleep
                time.sleep(int(msg[1])/1000.0)
            elif command == 'dump' and len(msg) == 1:    # print all the key-value pairs in shared memory
                for key,value in sorted(share_V.items()):
                    print (key,':',value)
        else:
            time.sleep(0.01)
            pass


###### Main Program Starts here ####################
# read the config file
with open('config.txt') as f:
    content = f.readlines()


# save the min_delay and max_delay in two varibles
min_delay, max_delay = content[0].strip().split()
min_delay, max_delay = int(min_delay), int(max_delay)

# save the other information in the port_info list
port_info = []
for i in content[1:-2]:
    port_info.append(i.strip().split())

# get the process IP and port info based on the selected number
def process_info(number):
    address = port_info[number][1]
    port = int(port_info[number][2])
    return (address, port)

addr_list = []
for i in range(7):
    addr_list.append(process_info(i))
# Initialize the process information: process number, host address, and IP
# address

P_ID= 9999
while P_ID not in {0,1,2,3,4,5,6,7}:
    P_ID= input('Select the process number from 0-7:' )
    P_ID= int(P_ID)

print('The process number selected is: {}'.format(P_ID))

# log_file function
file_name = 'log' + str(P_ID) + '.txt'

def write_to_file(name, text):
    log_file = open(name, "a+")
    log_file.write(text+'\r\n')
    log_file.close()


# Assign a process ID to perform as the leader that enable total ordering
leader_ID = 0
# define a dictionary to store message with marker in the memory.
msg_memory = {}
# Initiate a dictionary to store the shared key-value paris.
share_V= {'a':0,'b':0,'c':0,'d':0,'e':0,'f':0,'g':0,'h':0,'i':0,'j':0,
          'k':0,'l':0,'m':0,'n':0,'o':0,'p':0,'q':0,'r':0,'s':0,'t':0,
          'u':0,'v':0,'w':0,'x':0,'y':0,'z':0}
# Define a queue to store all the valid input commands:
Command_buff = queue.Queue()
# Define a flag to indicate whether an operation at the client has been invoked
invoke = True


# bind socket to the ip address based on the config file
s= socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(addr_list[P_ID])

# a thread is used to recieve incoming message
if P_ID == leader_ID:
    receive_thread = threading.Thread(target=total_listen_leader, args=(s,))
    receive_thread.start()
else:
    receive_thread = threading.Thread(target=total_listen_other, args=(s,))
    receive_thread.start()

# one thread is used to deliever msg in the buffer
deliver_thread= threading.Thread(target = Msg_deliver)
deliver_thread.start()

# one thread is used to process the commands from client in the buffer
client_thread= threading.Thread(target = client_to_file, args=(s,))
client_thread.start()

# the main program is used for clinet to take input message and process it
Total_order_send_to_leader()
