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

HOST_IP = socket.gethostbyname(socket.gethostname())


def heartbeat():
    while True:
        a = str(datetime.datetime.now())
        aa = []
        aa.append(a)
        myfile = open('replica_heartbeat.csv', 'w')
        with myfile:
            writer = csv.writer(myfile)
            writer.writerows([aa])
        time.sleep(1)


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
                send_checkpoint()
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


def send_data():
    time.sleep(5)
    host = R2_ADDR  # as both code is running on same pc
    port = 5000  # socket server port number
    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the serv
    message = str(
        state + ' ' + last_client1_seq_no + ' ' + last_client2_seq_no + ' ' + str(no_of_messages_updated))  # take input
    print("Sending Message: " + message)
    client_socket.send(message.encode())
    time.sleep(1)
    data = client_socket.recv(1024).decode()  # receive response
    client_socket.close()  # close the connection
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


def send_log():
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global membership
    global checkpoint_flag
    global membership_no
    global no_of_messages_updated
    global consistency_flag
    time.sleep(5)
    host = R2_ADDR  # as both code is running on same pc
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
    time.sleep(1)
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
    checkpoint_flag = 1
    print("BLOCKING: QUIESCENT STATE")
    if membership_no == 1:
        send_data()
    else:
        receive_data()
    print("SENDING LOGS")
    if log_flag == 0:
        log_flag = 1
        if membership_no == 1:
            send_log()
        else:
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
    while True:
        thread.start_new_thread(server_program_rm, ())
        thread.start_new_thread(heartbeat, ())
        server_program_client()