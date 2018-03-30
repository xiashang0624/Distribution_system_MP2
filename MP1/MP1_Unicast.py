import socket, time, threading
from random import randint
import pdb


# This program is designed to perform unicast with a option to allow delay.

# define a function to listen
def Listen(server_socket):
    while True:
        try:
            recieve_msg,addr = server_socket.recvfrom(1024)
            recieve_time = time.asctime().split()[3]
            print ('Received '+ str('"') + str(recieve_msg.decode('utf-8')) + str('"') +
                   ' from process {}, system time is '.format(addr[-1]%10) + recieve_time)
        except:
            time.sleep(0.05)
            pass


# define a function to unicast
def Unicast(client_socket):
    while True:
        message = input()
        msg = message.split()
        if not msg or msg[0] != 'send':
            print('Error input, unicast message should use the following format: send destination msg')
            pass
        else:
            target = int(msg[1])
            message = message[7:] # remove send and target number from input string
            send_time = time.asctime().split()[3]
            print ('Send '+ str('"') + message + str('"') +
                   ' to process {}, system time is '.format(target)+ send_time)
            Delay(client_socket, target, message)

# implement the delay mechanism
# if we want to reomve the delay mechasnim, set the delay_time to 0
def Delay(client_socket, target, message):
    delay_time = randint(min_delay, max_delay)/1000.0 #set it to 0 to remove the delay mechanism
    time.sleep(delay_time)
    client_socket.sendto(message.encode('utf-8'), addr_list[target])


# Unordered multicast
def Multicast_unorder(client_socket):
    while True:
        message = input()
        for i in range(4):
            client_socket.sendto(message.encode('utf-8'),addr_list[i])


# Initialize the process information: process number, host address, and IPk
# address

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


process_number = 9999
while process_number not in {1,2,3,0}:
    process_number = input('Select the process number from 0-3:' )
    process_number = int(process_number)

print('The process number selected is: {}'.format(process_number))

# bind socket to the ip address based on the config file
s= socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(addr_list[process_number])

# a thread is used to recieve incoming message
receive_thread = threading.Thread(target=Listen, args=(s,))
receive_thread.start()

# Unicast
Unicast(s)
# the main program for multicast message
#Multicast_unorder(s)
