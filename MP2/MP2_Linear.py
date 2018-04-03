import socket, time, threading
from random import randint
import pdb

# Key-Value Store consistency with linearizability.

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
                marked_msg = 'Marker ' + str(mark) + ' :received '+ str('"') +\
                              str(decode_msg) + str('"') +' from process {}'.format(addr[-1]%10)
                mark += 1
                cast_thread = threading.Thread(target = Multicast_from_leader, args=(server_socket, marked_msg))
                cast_thread.start()
            else:
                # save the message with time stamp in the buffer (msg_memory)
                msg_index_s = decode_msg.find(':') # find the starting index based on the symbol of colon
                recieved_marker = int(decode_msg.split()[1]) # get the index of marker in the msg
                recieve_time = ', system time is: ' + time.asctime().split()[3]
                msg_to_buffer = {recieved_marker: decode_msg[msg_index_s+1:] + recieve_time}
                msg_memory.update(msg_to_buffer)
        except:
            time.sleep(0.01)
            pass


# Total order leader broadcast msg,
def Multicast_from_leader(client_socket, message, leader_ID=0):
    for i in range(4):
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
            recieve_time = ', system time is: ' + time.asctime().split()[3]
            msg_to_buffer = {recieved_marker: decode_msg[msg_index_s+1:] + recieve_time}
            msg_memory.update(msg_to_buffer)
        except:
            time.sleep(0.05)
            pass


def Msg_deliver():
    mark_deliver = 0
    global msg_memory
    while True:
        if msg_memory and min(msg_memory) == mark_deliver:
            print (msg_memory[mark_deliver])
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
def Total_order_send_to_leader(client_socket, leader=0):
    while True:
        message = input()
        msg = message.split()
        if not msg:
            print('Error input, input should use the following format: put/get/delay/dump + key')
            pass
        else:
            send_time = int(time.time()*1000)  # get the timestampt in millisecond
            command= msg[0]
            if command == 'get' and len(msg)==2 and msg[1] in share_V: # write the get command to log file and broadcast
                key = msg[1]
                log_file.write('555,'+str(P_ID)+','+command+','+key+','+str(send_time)+','+'req'+',\r\n')
                Send_Delay(client_socket, leader, message)
            elif command == 'put' and len(msg) == 3 and msg[1] in share_V:          # write the put command to log file and broadcast
                key,value = msg[1:2]
                log_file.write('555,'+str(P_ID)+','+command+','+key+','+str(send_time)+','+'req'+','+str(value)+',\r\n')
                Send_Delay(client_socket, leader, message)
            elif command == 'delay' and len(msg) == 2:        # put the stdin sleep
                time.sleep(int(msg[1])/1000.0)
            elif command == 'dump' and len(msg) == 1:    # print all the key-value pairs in shared memory
                for key,value in sorted(share_V.items()):
                    print (key,':',value)
            else:
                print('Error input, input should use the following format: put/get/delay/dump + (key + value).')
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
for i in range(4):
    addr_list.append(process_info(i))
# Initialize the process information: process number, host address, and IP
# address

P_ID= 9999
while process_number not in {0,1,2,3,4,5,6,7}:
    P_ID= input('Select the process number from 0-7:' )
    P_ID= int(P_ID)

print('The process number selected is: {}'.format(process_number))

# initiate the log file:
file_name = 'log' + str(P_ID) + '.txt'
log_file = open(file_name,"w+")


# Assign a process ID to perform as the leader that enable total ordering
leader_ID = 0
# define a dictionary to store message with marker in the memory.
msg_memory = {}
# Initiate a dictionary to store the shared key-value paris.
share_V= {'a':0,'b':0,'c':0,'d':0,'e':0,'f':0,'g':0,'h':0,'i':0,'j':0,
          'k':0,'l':0,'m':0,'n':0,'o':0,'p':0,'q':0,'r':0,'s':0,'t':0,
          'u':0,'v':0,'w':0,'x':0,'y':0,'z':0}

# bind socket to the ip address based on the config file
s= socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(addr_list[process_number])

# a thread is used to recieve incoming message
if process_number == leader_ID:
    receive_thread = threading.Thread(target=total_listen_leader, args=(s,))
    receive_thread.start()
else:
    receive_thread = threading.Thread(target=total_listen_other, args=(s,))
    receive_thread.start()

# one thread is used to deliever msg in the buffer
deliver_thread= threading.Thread(target = Msg_deliver)
deliver_thread.start()

# the main program is to broadcast the message
Total_order_send_to_leader(s, leader_ID)
