# listen() has not been debugged completely

import socket, time, threading
from random import randint
import pdb

# get the process IP and port info based on the selected number
def process_info(number):
    address = port_info[number][1]
    port = int(port_info[number][2])
    return address, port

# define a function to listen
def listen(server_socket):
    global localMarker
    # buf_recv is a receive_buffer for message received which is not causally in order
    #  for now, and the key for this is the message marker(timestamp )
    buf_recv ={}
    while True:
        try:
            receive_msg,addr = server_socket.recvfrom(1024)
            # message received is divided into three part, the msg + addr  +
            # and we can differ the id by its port information
            src_port = addr[-1]%10
            receive_msg = str(receive_msg.decode('utf-8'))
            receive_msg = receive_msg.split(',')
            marker_recv = receive_msg[0].split()
            t = []
            for i in range(capacity_room):
                t.append(int(marker_recv[i]))
            marker_recv = (t[0], t[1], t[2], t[3])
            receive_msg = receive_msg[1]
            receive_time = time.asctime().split()[3]
            deliver_flag = 0
            deliver_flag = marker_cmp(t,  src_port)
            if deliver_flag == 1:
                print ('Received '+ str('"') + str(receive_msg) + str('"') +
                   ' from process {}, system time is '.format(src_port) + receive_time)
                marker_add(src_port)
                # after get the message delivered, there should be a circular check on those buffered message.
                buf_msg_key = list(buf_recv.keys())
                for i in range(len(buf_msg_key)):
                    temp = buf_msg_key[i]
                    buf_out_flag = 0
                    buf_out_flag = marker_cmp(temp,  buf_recv[temp][1])
                    if buf_out_flag == 1:
                        print('Received (buffered) ' + str('"') + str(buf_recv[temp][0]) + str('"') +
                              ' from process {}, system time is '.format(buf_recv[temp][1]) + buf_recv[temp][2])
                        marker_add(buf_recv[temp][1])
                        # if we retrieve a buffer, then add the timestamp by 1 on that port
                        buf_recv.pop(temp)
                    else:
                        continue
            else:
                buf_recv[marker_recv] = (receive_msg, src_port, receive_time)

        except:
        # # normally with certain exception kind, other than barely use the exception
        #     time.sleep(0.05)
            pass


# define a function to unicast to specified destination
def Unicast(client_socket, destination):

            global message_wrapped
            # send_time = time.asctime().split()[3]
            # print ('Send '+ str('"') + str(message_wrapped).split(',')[1] + str('"') +
            #        ' to process {}, system time is '.format(destination)+ send_time)

            # should be deleted if we only want one message to signal the completion of sending
            delay_thread = threading.Thread(target=Delay, args=(client_socket, destination,))
            delay_thread.start()

# the delay function is to delay the send operation for a random time defined by configuration file
def Delay(client_socket, target):

    delay_time = randint(min_delay, max_delay)/1000.0
    time.sleep(delay_time)
    client_socket.sendto(message_wrapped.encode('utf-8'),addr_list[target])
    # sendto function only cares about the message and its destination addr
    # Unordered multicast


# the function for causal_order multicast, which wiill call thread unicast to multicast
def Causal_order(client_socket):
    global localMarker
    global message_wrapped
    while True:
        message = input()
        msg = message.split()
        if msg[0] == 'check':
            print(localMarker)
        if not msg or msg[0] != 'msend':
            print('Error input, unicast message should use the following format: msend msg')
            pass
        else:
            print('Receive ' + str('"') +  msg[0]  + str('"') +
                  ' from process {}, system time is '.format(process_number) + time.axctime().split()[3])
            marker_add(process_number)
            message = message[6:]  # remove send and target number from input string
            message_wrapped = str(localMarker[0]) + ' ' + str(localMarker[1]) + ' ' + \
                              str(localMarker[2]) + ' ' + str(localMarker[3]) + ',' + message

            for i in destination_group:
                Unicast(client_socket, i)
            send_time = time.asctime().split()[3]
            print('Send ' + str('"') + str(message_wrapped).split(',')[1] + str('"') +
                  ' to process {}, system time is '.format(destination_group) + send_time)
# after delivering a message from one port, update the marker information in local marker variable
def marker_add(port):
    global localMarker
    a = []
    for i in range(capacity_room):
        a.append(localMarker[i])
    a[port] += 1
    localMarker = (a[0], a[1], a[2], a[3])


# function to compare message marker with local marker, if the it is in right causal-order of localmaker
# it return 1, if the message is nor right causal-order, then return -1.
def marker_cmp(a, port):
        global localMarker
        flg = 0
        if a[port] == (localMarker[port]+1):
            flg = 1
            for i in range(capacity_room):
                if i == port:
                    continue
                else:
                    if a[i] > localMarker[i]:
                        flg = 0
            return flg
        else:
            return flg
        # return 1

# read the config file
with open('config.txt') as f:
    content = f.readlines()

capacity_room = 4

# save the min_delay and max_delay in two varibles
min_delay, max_delay = content[0].strip().split()
min_delay, max_delay = int(min_delay), int(max_delay)

# save the other information in the port_info list
port_info = []
for i in content[1:-2]:
    port_info.append(i.strip().split())

addr_list = []
for i in range(capacity_room):
    addr_list.append(process_info(i))



# Initialize the process information: process number, host address, and IPk
# address
process_number = 9999
while process_number not in {1,2,3,0}:
    process_number = input('Select the process number from 0-3:' )
    process_number = int(process_number)
print('The process number selected is: {}'.format(process_number))
destination_group = []
for i in range(capacity_room) :
    if i != process_number:
        destination_group.append(i)


# add a global var that represents its causal order for now
localMarker = (0,0,0,0)
message_wrapped = ''

# bind socket to the ip address based on the config file
s= socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(addr_list[process_number])

# a thread is used to recieve incoming message
receive_thread = threading.Thread(target=listen, args=(s,))
receive_thread.start()

# the main for causal_order
Causal_order(s)
