import socket, time, threading
from random import randint

# Eventual Consistency key-value system is to make the Write and Read opearation request from
# different process to:
# 1. write with the timestamp
# 2. get the output by latest-write wins,
# both of which follow the eventual consistency model.

# indicate if the eventual consistency is  tempeorarily met
global waiting_flag
waiting_flag = False

write_buffer = {}
read_buffer = {}
operation_buffer = []

# arbitray_log_num for index when us of the visualization tool
arbitray_log_num = 666

# initialize a dictionary for storing key-value: value = (val, write_timestamp)
replica = {}
for i in range(26):
    a = chr(97 + i)
    replica[a] = (0, str(0))
    write_buffer[a] = 0
    read_buffer[a] = []

# define the number of replica, the parameter W and R
temp = input('decide on parameter:\n in the format like below\nW,R,replica_number\n')
temp1 = temp.split(',')
W, R, number_replica = int(temp1[0]), int(temp1[1]), int(temp1[2])


# ***************** client starts here  ********************
# main thread used for buffer all the input instruction for client to operate
def operation_input():
    while True:
            message = input('input your command:\n')
            msg = message.split()
            if msg[0] == 'exit':
                print('manually exit ')
                exit()
            elif msg[0] == 'delay':
                time.sleep(int(msg[1]))
            elif msg[0] in {'put', 'get', 'dump'}:
                 operation_buffer.append(msg)
            else:
                print('Invalid input')


# client is thread for client operation, it gets the operation from the operation
# buffer
def client():
    while(True):
        if len(operation_buffer) != 0:
            msg = operation_buffer.pop(0)
            if msg[0] == 'put':
                put(msg[1], int(msg[2]))
            elif msg[0] == 'get':
                output = get(msg[1])
            # match the command to different function to manipulate replica
            elif msg[0] == 'dump':
                dump()
        time.sleep(0.05)


def put(writeVar, writeVal):
    global waiting_flag
    req_time = time.asctime().split()[3].replace(':','')
    message = 'write' + ',' + writeVar + ',' + str(writeVal) + ',' + str(req_time)
    if not writeVar in list(replica.keys()):  # delete
        print('wrong input for \'write\' command')
        # IOError('input error')
    else:
        log = str(arbitray_log_num) + ',' + str(
            process_number) + ',put,' + writeVar + ',' + req_time + ',req' + ',' + str(writeVal)
        write_to_file(file_name, log)
        waiting_flag = True
        Multicast_unorder(s, message)
        # keeps on receiving acknowledgement until number of that is larger than
        while (waiting_flag):
            time.sleep(0.05)

        req_time = time.asctime().split()[3].replace(':', '')
        log = str(arbitray_log_num) + ',' + str(
            process_number) + ',put,' + writeVar + ',' + req_time + ',resp' + ',' + str(writeVal)
        write_to_file(file_name, log)


def get(readVar):
    global waiting_flag
    message = 'read' + ',' + readVar
    req_time = time.asctime().split()[3].replace(':', '')
    if not readVar in list(replica.keys()):  # delete
        print('wrong input')
        return 0
    else:
        log = str(arbitray_log_num) + ',' + str(process_number) + ',get,' + readVar + ',' + req_time + ',req' + ','
        write_to_file(file_name, log)
        waiting_flag = True
        Multicast_unorder(s, message)
        while (waiting_flag):
            time.sleep(0.05)

        req_time = time.asctime().split()[3].replace(':', '')
        # choose the val with the latest timestamp
        x, y = max(read_buffer[readVar])
        log = str(arbitray_log_num) + ',' + str(
            process_number) + ',get,' + readVar + ',' + req_time + ',resp' + ',' + str(y)
        write_to_file(file_name, log)
        return y


# print all variables and their values stored in the local replica
def dump():
    req_time = time.asctime().split()[3].replace(':', '')
    print('dump ' + req_time)
    # write_to_file(file_name, 'dump ' + req_time)
    for i in replica.keys():
        val1, val2 = replica[i]
        log = str(i) + ' ' + str(val1) + ' ' + val2
        print(log)
        # write_to_file(file_name, log)
    print('input your command:')


# prevent prompt input for several miliseconds
def delay(interval):
    time.sleep(interval)


# ***********replica****************
def replica_req(var, src_process):
    local_val, timestamp = replica[var]
    ack_read = 'ack_read' + ',' + var + ',' + str(local_val) + ',' + timestamp
    Unicast(s, src_process, ack_read)


def replica_update(var, val, timestamp, src_process):
    local_val, loc_val_ts = replica[var]
    # last write wins
    if timestamp > loc_val_ts:
        replica[var] = (val, timestamp)
    else:
        pass
    ack_write = 'ack_write' + ',' + var
    Unicast(s, src_process, ack_write)


# following part is replica_server to listen any operations for modifying the replica
def replica_server(server_socket):
    global waiting_flag
    while True:
        try:
            recieve_msg,addr = server_socket.recvfrom(1024)
            receive_src = addr[-1]%10
            receive_str = recieve_msg.decode('utf-8')
            msg = receive_str.split(',')
            operata = msg[0]
            # recv_time = time.asctime().strip().split()[3]
            if operata == 'write':
                replica_update(msg[1], int(msg[2]), msg[3], receive_src)

            elif operata == 'read':
                replica_req(msg[1], receive_src)

            elif operata == 'ack_read':
                # when ack_read for same variables is received more than R times, then it permits new operators
                # buffer is reply message for reading which we have received so far, value is ( timestamp, value )
                read_buffer[msg[1]].append((msg[3], int(msg[2])))
                if len(list(read_buffer[msg[1]])) == R:
                    # log_file.write()
                    waiting_flag = False
                elif len(read_buffer[msg[1]]) == number_replica:
                    read_buffer[msg[1]] = []

            elif operata == 'ack_write':
                # when ack_write for some operators to write more than W times, then it will be ok.
                write_buffer[msg[1]] += 1
                if write_buffer[msg[1]] == W:
                    waiting_flag = False
                elif write_buffer[msg[1]] == number_replica:
                    write_buffer[msg[1]] = 0
        except:
            pass


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
            # for t in threading.enumerate():
            #     print(t.name + 'threading for ' + str(i) + 'th multicast ')


# **********Main**************
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
    process_number = input('choose the process num from 0-6:' )
    process_number = int(process_number)
print('The process number selected is: {}'.format(process_number))


# initialize a log file
file_name = 'log' + str(process_number) + '.txt'


# write log file
def write_to_file(name, text):
    log_file = open(name, "a+")
    log_file.write(text+'\r\n')
    log_file.close()


# bind socket to the ip address based on the config file
s= socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(addr_list[process_number])

replica_thread = threading.Thread(target=replica_server, args=(s,))
replica_thread.start()

client_thread = threading.Thread(target=client)
client_thread.start()
operation_input()