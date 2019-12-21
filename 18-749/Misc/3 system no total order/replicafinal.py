import datetime
import csv
import socket
import time
import _thread as thread

global last_client1_seq_no
global last_client2_seq_no
global state
global membership
global checkpoint_flag
global log_flag
global membership_no
global no_of_messages_updated
global consistency_flag
global R2_ADDR
global REPLICA_IP_LIST
global PREV_REPLICA_IP_LIST
global NEW_REPLICAS_IP
global CONSISTENT

HOST_IP = socket.gethostbyname(socket.gethostname())
HEARTBEAT_FREQ = 1

def heartbeat():
    while True:
        a = str(datetime.datetime.now())
        aa = []
        aa.append(a)
        myfile = open('replica_heartbeat.csv', 'w')
        with myfile:
            writer = csv.writer(myfile)
            writer.writerows([aa])
        time.sleep(HEARTBEAT_FREQ)

def s_v(client_id, client_seq_no, client_message, ip)
    time.sleep(2)
    host = ip  # as both code is running on same pc
    port = 6000  # socket server port number
    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the serv
    message = str(
        str(client_id) + ' ' + str(client_seq_no) + ' ' + str(client_message))  # take input
    print("Sending Message: " + message)
    client_socket.send(message.encode())
    time.sleep(1)
    data = client_socket.recv(1024).decode()  # receive response
    client_socket.close()  # close the connection
    return

def send_vote(client_id, client_seq_no, client_message):
    global REPLICA_IP_LIST
    print("SENDING VOTE")
    for ip in REPLICA_IP_LIST:
        thread.start_new_thread(s_v, (client_id, client_seq_no, client_message, ip))
   
    pass


def receive_vote():
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global no_of_messages_updated
    host = HOST_IP
    port = 6000  # initiate port no above 1024
    server_socket = socket.socket()  # get instance
    server_socket.bind((host, port))  # bind host address and port together
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.listen(0)
    a = 1
    print("RECEIVING VOTE")
    while a == 1:
        conn, address = server_socket.accept()  # accept new connection
        while True:
            data = conn.recv(1024).decode()
            if not data:
                a = 0
                break
            recemsg = str(data)
            recemsgsplit = recemsg.split(' ')
            id = recemsgsplit[0]
            seq_no = int(recemsgsplit[1])
            mess = int(recemsgsplit[2])
            a = 0
            break
    conn.close()  # close the connection
    return id, seq_no, mess


def total_order(client_id, client_seq_no, client_message):
    return

    global membership_no
    global no_of_messages_updated
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    print("TOTAL ORDERING")
    if membership_no == 1:
        send_vote(client_id, client_seq_no, client_message)
        id, seq_no, mess = receive_vote()
    else:
        id, seq_no, mess = receive_vote()
        send_vote(client_id, client_seq_no, client_message)
    if seq_no == client_seq_no:
        state = client_message
        no_of_messages_updated = int(no_of_messages_updated) + 1
        printline = str("The latest state is " + str(state))
        print(printline)
        if client_id == 'c1':
            last_client1_seq_no = int(client_seq_no)
        else:
            last_client2_seq_no = int(client_seq_no)
    else:
        state = client_message
        no_of_messages_updated = int(no_of_messages_updated) + 1
        printline = str("The latest state is " + str(state))
        print(printline)
        if client_id == 'c1':
            last_client1_seq_no = int(client_seq_no)
        else:
            last_client2_seq_no = int(client_seq_no)
        state = mess
        no_of_messages_updated = int(no_of_messages_updated) + 1
        printline = str("The latest state is " + str(state))
        print(printline)
        if id == 'c1':
            last_client1_seq_no = int(seq_no)
        else:
            last_client2_seq_no = int(seq_no)
    pass



def server_program_client():
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global membership
    global checkpoint_flag
    global membership_no
    global no_of_messages_updated
    global consistency_flag
    global log_flag
    host = HOST_IP
    port = 6897  # initiate port no above 1024
    write_port(port)
    server_socket = socket.socket()  # get instance
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))  # bind host address and port together
    server_socket.listen(5)
    print('Waiting for message from Client')
    a = 1
    t = []
    membercheck = []
    ipaddr = []
    while a == 1:
        conn, address = server_socket.accept()  # accept new connection
        while True:
            data = conn.recv(1024).decode()
            if not data:
                a = 0
                break
            recemsg = str(data)  ##### MESSAGE FORM: 'client id, sequence number, 'message'
            recemsgsplit = recemsg.split(' ')
            client_id = recemsgsplit[0]
            client_seq_no = recemsgsplit[1]
            client_message = recemsgsplit[2]
            printst = str("Message from client " + str(client_id) + " with sequence number " + str(
                client_seq_no) + ". Message is " + str(client_message))
            print(printst)
            t.append(client_id)
            t.append(client_seq_no)
            t.append(client_message)
            myfile = open('logged_data.csv', 'a')
            with myfile:
                writer = csv.writer(myfile)
                writer.writerows([t])
            with open('membership.csv') as csvDataFile:
                csvReader = csv.reader(csvDataFile)
                for row in csvReader:
                    if row:
                        membercheck.append(row[0])
                        ipaddr.append(row[2])
            mem = int(membercheck[0])
            if mem == 1:
                update_state(client_message, client_seq_no, client_id)
            else:
                consistency_flag = 0
                if not CONSISTENT:
                    send_checkpoint()
                else:
                    total_order(client_id, client_seq_no, client_message)
            conn.send(data.encode())  # send data to the clientx
            a = 0
            break
    conn.close()  # close the connection


def write_port(port):
    x = [port]
    myfile = open('replica_port.csv', 'w')
    with myfile:
        writer = csv.writer(myfile)
        writer.writerows([x])


def update_state(client_message, client_seq_no, client_id):
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global membership
    global checkpoint_flag
    global membership_no
    global no_of_messages_updated
    global log_flag
    membership_no = 1
    checkpoint_flag = 0
    log_flag = 0
    state = int(client_message)
    no_of_messages_updated = int(no_of_messages_updated) + 1
    printline = str("The latest state is " + str(state))
    print(printline)
    if client_id == 'c1':
        last_client1_seq_no = int(client_seq_no)
    else:
        last_client2_seq_no = int(client_seq_no)
    pass


def server_program_rm():
    # get the hostname
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global membership
    global R2_ADDR
    global REPLICA_IP_LIST
    global PREV_REPLICA_IP_LIST
    global CONSISTENT
    host = HOST_IP
    port = 6887  # initiate port no above 1024
    server_socket = socket.socket()  # get instance
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))  # bind host address and port together
    server_socket.listen(0)
    a = 1
    while a == 1:
        conn, address = server_socket.accept()  # accept new connection
        while True:
            t = []
            data = conn.recv(1024).decode()
            if not data:
                a = 0
                break
            print("from connected user: " + str(data))
            recemsg = str(data)
            recemsgsplit = recemsg.split(' ')
            membershipcount = recemsgsplit[0]
            ip_addr_self = recemsgsplit[1]
            ip_addr_r2 = recemsgsplit[2]
            t.append(membershipcount)
            t.append(ip_addr_self)
            t.append(ip_addr_r2)

            #new replica, create PREV_REPLICA_LIST
            if PREV_REPLICA_IP_LIST == []:
                PREV_REPLICA_IP_LIST = t.copy()
                for i in range(len(PREV_REPLICA_IP_LIST)):
                    if host == PREV_REPLICA_IP_LIST[i]:
                        PREV_REPLICA_IP_LIST[i] = "0"
                CONSISTENT = False


            #only update previous replica if consistency is true
            if CONSISTENT:
                PREV_REPLICA_IP_LIST = REPLICA_IP_LIST.copy()

            REPLICA_IP_LIST = t.copy()
            NEW_REPLICAS_IP = []
            #get new replicas to receive checkpoint
            for i in range(len(PREV_REPLICA_IP_LIST)):
                #replica comes up
                if PREV_REPLICA_IP_LIST[i] == "0" and PREV_REPLICA_IP_LIST[i] != REPLICA_IP_LIST[i]:
                    CONSISTENT = False
                    NEW_REPLICAS_IP.append(REPLICA_IP_LIST[i])


            

            R2_ADDR = ip_addr_r2
            myfile = open('membership.csv', 'w')
            with myfile:
                writer = csv.writer(myfile)
                writer.writerows([t])
            if int(recemsgsplit[0]) != membership:
                print("Membership change")
                print(str("Total Members: " + recemsgsplit[0]))
                membership = int(recemsgsplit[0])
            conn.send(data.encode())  # send data to the client
            break
    conn.close()  # close the connection

def s_d(addr):
    time.sleep(2)
    host = addr  # as both code is running on same pc
    port = 5000  # socket server port number
    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the serv
    message = str(
        str(state)+ ' ' + str(last_client1_seq_no) + ' ' + str(last_client2_seq_no) + ' ' + str(no_of_messages_updated))  # take input
    print("Sending Message: " + message)
    client_socket.send(message.encode())
    time.sleep(1)
    data = client_socket.recv(1024).decode()  # receive response
    client_socket.close()  # close the connection
    return

def send_data():
    global NEW_REPLICAS_IP
    for addr in NEW_REPLICAS_IP:
        thread.start_new_thread(s_d, addr)
    pass


def receive_data():
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global no_of_messages_updated
    host = HOST_IP
    port = 5000  # initiate port no above 1024
    server_socket = socket.socket()  # get instance
    server_socket.bind((host, port))  # bind host address and port together
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.listen(0)
    a = 1
    while a == 1:
        conn, address = server_socket.accept()  # accept new connection
        while True:
            data = conn.recv(1024).decode()
            if not data:
                a = 0
                break
            recemsg = str(data)
            recemsgsplit = recemsg.split(' ')
            state = int(recemsgsplit[0])
            last_client1_seq_no = int(recemsgsplit[1])
            last_client2_seq_no = int(recemsgsplit[2])
            no_of_messages_updated = int(recemsgsplit[3])
            printline = str("The latest state is " + str(state))
            print(printline)
            a = 0
            break
    conn.close()  # close the connection
    pass

def s_l(addr):
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global membership
    global checkpoint_flag
    global membership_no
    global no_of_messages_updated
    global consistency_flag
    time.sleep(2)
    #send logs to all new replicas
    
        host = addr  # as both code is running on same pc
        port = 5001  # socket server port number
        client_socket = socket.socket()  # instantiate
        client_socket.connect((host, port))  # connect to the server
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        f = open("logged_data.csv", "rb")
        l = f.read(1024)
        while l:
            client_socket.send(l)
            l = f.read(1024)
        f.close()
        client_socket.close()  # close the connection
    pass

def s_l(addr):

    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global membership
    global checkpoint_flag
    global membership_no
    global no_of_messages_updated
    global consistency_flag
    time.sleep(2)
    #send logs to all new replicas
    host = addr  # as both code is running on same pc
    port = 5001  # socket server port number
    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server
    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    f = open("logged_data.csv", "rb")
    l = f.read(1024)
    while l:
        client_socket.send(l)
        l = f.read(1024)
    f.close()
    client_socket.close()  # close the connection
    return


def send_log():
    global NEW_REPLICAS_IP
    for addr in NEW_REPLICAS_IP:
        thread.start_new_thread(s_l, addr)




def receive_log():
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global membership
    global checkpoint_flag
    global membership_no
    global no_of_messages_updated
    global consistency_flag

    host = HOST_IP
    port = 5001  # initiate port no above 1024
    i = 1
    server_socket = socket.socket()  # get instance
    server_socket.bind((host, port))  # bind host address and port together
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.listen(2)
    conn, address = server_socket.accept()  # accept new connection
    print("Connection from: " + str(address))
    with open('received.csv', 'wb') as f:
        time.sleep(1)
        for i in range(300):
            data = conn.recv(1024)
            f.write(data)
            i = i + 1
    f.close()
    conn.close()  # close the connection
    pass


def send_checkpoint():
    global checkpoint_flag
    global membership_no
    global log_flag
    global REPLICA_IP_LIST
    global PREV_REPLICA_IP_LIST
    global NEW_REPLICAS_IP
    host = HOST_IP

    sender = ""
    for i in range(len(PREV_REPLICA_IP_LIST)):
            if PREV_REPLICA_IP_LIST[i] != "0":
                sender = PREV_REPLICA_IP_LIST[i]
                break

    checkpoint_flag = 1
    print("BLOCKING: QUIESCENT STATE")
    if host == sender:
        print("SENDING FINAL STATE")
        send_data()
    else:
        print("RECEIVING FINAL STATE")
        receive_data()

    if log_flag == 0:
        log_flag = 1
        if host == sender:
            print("SENDING LOGS")
            send_log()
        else:
            print("RECEIVING LOGS")
            receive_log()
    print("UNBLOCKING")
    pass


if __name__ == '__main__':
    last_client1_seq_no = 0
    last_client2_seq_no = 0
    state = 0
    membership = 1
    checkpoint_flag = 0
    membership_no = 2
    no_of_messages_updated = 0
    consistency_flag = 0
    log_flag = 0
    PREV_REPLICA_IP_LIST = []
    CONSISTENT = False
    while True:
        thread.start_new_thread(server_program_rm, ())
        thread.start_new_thread(heartbeat, ())
        server_program_client()
