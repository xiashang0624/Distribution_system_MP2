import socket, time, threading
from random import randint

# Eventual Consistency key-value system is to make the Write and Read opearation from
# different process to:
# 1. write with the timestamp
# 2. get the output by latest-write wins,
# both of which follow the eventual consistency model.

write_buffer = {}
read_buffer = {}
ack_buffer = []
waiting_flag = False
arbitray_log_num = 666

# initialize a dictionary for storing key-value: value = (val, write_timestamp)
replica = {}
for i in range(26):
    a = chr(65 + i)
    replica[a] = (0, 0)
    write_buffer[a] = 0
    read_buffer[a] = []



# *****************client ********************
# we will initialize the client instance to make a
def client():
    while(True):
            message = input('input your message')
            msg = message.split()
            if msg[0] == 'put':
                put(msg[1], msg[2])
                print('put finished:write ' + msg[1] + ' to be ' + msg[2])
            elif msg[0] == 'get':
                output = get(msg[1])
                print('get finished:' + msg[1] + ' is ' + output)
            # match the command to different function to manipulate replica
            elif msg[0] == 'dump':
                dump()

            elif msg[0] == 'delay':
                time.sleep(int(msg[1]))

            else:
                print('Invalid input')


def put(writeVar, writeVal):
    req_time = time.asctime().split()[3]
    log_file.write(
        str(arbitray_log_num) + ',' + str(process_number) + ',put,' + writeVar + ',' + req_time + ',req' + ',' + writeVal)
    waiting_flag = True
    message = 'write' + ',' + writeVar + ',' + str(writeVal) + ',' + str(req_time)
    if not writeVar in list(replica.keys()):  # delete
        print('wrong input for \'write\' command')
        pass
    else:
        Multicast_unorder(s, message)
    while (waiting_flag):
        time.sleep(0.05)
    log_file.write(
        str(arbitray_log_num) + ',' + str(process_number) + ',put,' + writeVar + ',' + req_time + ',req' + ',' + writeVal)
    # keeps on receiving acknowledgement until number of that is larger than

def get(readVar):
    req_time = time.asctime().split()[3]
    log_file.write(
        str(arbitray_log_num) + ',' + str(process_number) + ',get,' + readVar + ',' + req_time + ',req' + ',')
    waiting_flag = True
    message = 'read' + ',' + readVar
    if not readVar in list(replica.keys()):  # delete
        print('wrong input')
    else:
        Multicast_unorder(s, message)
    while (waiting_flag):
        time.sleep(0.05)
    # choose the val with the latest timestamp
    x, y = max(list[read_buffer.values()])
    log_file.write(
        str(arbitray_log_num) + ',' + str(process_number) + ',get,' + readVar + ',' + req_time + ',req' + ',' + y)
    return y

def dump():
    pass

def delay():
    pass



# ***********replica****************
def replica_req(var, src_process):
    local_val, timestamp = replica[var]
    ack_read = 'ack_read' + ',' + var + ',' + local_val + ',' + timestamp
    Unicast(s, src_process, ack_read)

def replica_update(var, val, timestamp, src_process):
    local_val, loc_val_ts = replica[var]
    # last write wins
    if timestamp > loc_val_ts:
        replica[var] = (val, timestamp)
        print('loacl copy of  + var +  'is 'updated')
    else:
        print('message is eliminated')
    ack_write = 'ack_write' + ',' + var
    Unicast(s, src_process,ack_write)


# following part is replica_server to listen any operations for modifying the replica
def replica_server(server_socket):
    while True:
        try:
            recieve_msg,addr = server_socket.recvfrom(1024)
            receive_src = addr[-1]%10
            receive_str = recieve_msg.decode('utf-8')
            msg = receive_str.split()
            operata = msg[0]
            recv_time = time.asctime().strip().split()[3]
            if operata == 'write':
                replica_update(msg[1], int(msg[2]), msg[3], receive_src)

            elif operata == 'read':
                replica_req(msg[1], receive_src)

            elif operata == 'ack_read':
                # when ack_read for same operators is received more than R times, then it will be ok.
                # buffer is "read reply" we have received so far, value is ( timestamp, value )

                read_buffer[msg[1]].append((msg[3], msg[2]))
                if len(read_buffer[msg[1]]) == R:
                    # log_file.write()
                    waiting_flag = False
                elif len(read_buffer[msg[1]]) == number_replica:
                    read_buffer[msg[1]] = []

            elif operata == 'ack_write':
                # when ack_write for some operators to write more than W times, then it will be ok.
                write_buffer[msg[1]] += 1
                if write_buffer[msg[1]] == W:
                    waiting_flag = True
                elif write_buffer[msg[1]] == number_replica:
                    write_buffer[msg[1]] = 0

            print (' from process {}, system time is '.format(receive_src) + recv_time)
        except:
            pass

# ****************broadcast with delay***************
# define a function to uni-cast
def Unicast(client_socket, target, message):
    send_thread = threading.Thread(target = Delay(), args = (client_socket, target, message,))
    send_thread.start()

# implement the delay mechanism
def Delay(client_socket, target, message):
    delay_time = randint(min_delay, max_delay)/1000.0 #set it to 0 to remove the delay mechanism
    time.sleep(delay_time)
    client_socket.sendto(message.encode('utf-8'), addr_list[target])

# Unordered multi-cast
def Multicast_unorder(client_socket, message):
        for i in range(number_replica):
            Unicast(client_socket, i, message)





# define the number of replica, the parameter W and R
number_replica = 4
W = 4
R = 4

# get the process IP and port info based on the selected number
def process_info(number):
    address = port_info[number][1]
    port = int(port_info[number][2])
    return (address, port)

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

addr_list = []
for i in range(number_replica):
    addr_list.append(process_info(i))

process_number = 9999
while not process_number in {1,2,3,4,5,6,0}:
    process_number = input('Select the process number from 0-3:' )
    process_number = int(process_number)
print('The process number selected is: {}'.format(process_number))


# initialize a log file
file_name = 'log' + str(process_number) + '.txt'
log_file = open(file_name,"w+")

# bind socket to the ip address based on the config file
s= socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(addr_list[process_number])

receive_thread = threading.Thread(target=replica_server(), args=(s,))
receive_thread.start()


