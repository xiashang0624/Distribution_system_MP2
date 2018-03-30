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
            time.sleep(0.05)
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


# implement the delay mechanism
def Delay(client_socket, target, message):
    delay_time = randint(min_delay, max_delay)/1000.0
    time.sleep(delay_time)
    client_socket.sendto(message.encode('utf-8'), addr_list[target])


def Total_order_send_to_leader(client_socket, leader = 0):
    while True:
        message = input()
        msg = message.split()
        if not msg or msg[0] != 'msend' or len(msg) <2:
            print('Error input, total cast should use the following format: msend message')
            pass
        else:
            send_time = time.asctime().split()[3]
            print ('Send Total-order Multicast '+ str('"') + message + str('"') +
                   ', system time is ' + send_time)
            Delay(client_socket, leader, message[6:])

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

process_number = 9999
while process_number not in {1,2,3,0}:
    process_number = input('Select the process number from 0-3:' )
    process_number = int(process_number)

print('The process number selected is: {}'.format(process_number))

# Assign a process ID to perform as the leader that enable total ordering
leader_ID = 0
# define a dictionary to store message with marker in the memory.
msg_memory = {}

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

# the main program is to send input msg to the leader
Total_order_send_to_leader(s, leader_ID)
