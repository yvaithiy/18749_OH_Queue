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
global membership_no
global no_of_messages_updated
global consistency_flag

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
    # get the hostname
    host = '128.237.186.243'
    #host = '127.0.0.1'
    # print (host)
    port = 6897  # initiate port no above 1024
    x = [port]
    myfile = open('replica_port.csv', 'w')
    with myfile:
        writer = csv.writer(myfile)
        writer.writerows([x])
    # print ('Port Stored')
    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))  # bind host address and port together
    # configure how many client the server can listen simultaneously
    server_socket.listen(5)
    print('Waiting for message from Client')
    a = 1
    t = []
    membercheck = []
    ipaddr = []
    while a == 1:
        conn, address = server_socket.accept()  # accept new connection
        while True:
            # receive data stream. it won't accept data packet greater than 1024 bytes
            data = conn.recv(1024).decode()
            if not data:
                a = 0
                # if data is not received break
                break
            # print("from connected user: " + str(data))
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
                print("Writing Data: " + str(t))
            with open('membership.csv') as csvDataFile:
                csvReader = csv.reader(csvDataFile)
                for row in csvReader:
                    if row:
                        membercheck.append(row[0])
                        ipaddr.append(row[2])
            mem = int(membercheck[0])
            # data = input(' -> ')
            if mem == 1:
                checkpoint_flag = 0
                membership_no = 1
                consistency_flag = 1
                state = int(client_message)
                no_of_messages_updated = int(no_of_messages_updated) + 1
                printline = str("The latest state is "+str(state))
                print(printline)
                if client_id == 'c1':
                    last_client1_seq_no = int(client_seq_no)
                else:
                    last_client2_seq_no = int(client_seq_no)
            else:
                consistency_flag = 0
            conn.send(data.encode())  # send data to the clientx
    conn.close()  # close the connection


def server_program_rm():
    # get the hostname
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global membership
    host = '128.237.186.243'
    #host = '127.0.0.1'
    # print(host)
    port = 6887  # initiate port no above 1024
    server_socket = socket.socket()  # get instance
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together
    # print ('Bind Complete')
    # configure how many client the server can listen simultaneously
    server_socket.listen(0)
    # print ('Listening.....')
    a = 1
    t = []
    while a == 1:
        conn, address = server_socket.accept()  # accept new connection
        while True:
            t = []
            # receive data stream. it won't accept data packet greater than 1024 bytes
            data = conn.recv(1024).decode()
            if not data:
                a = 0
                # if data is not received break
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
            myfile = open('membership.csv', 'w')
            with myfile:
                writer = csv.writer(myfile)
                writer.writerows([t])
                print("Writing Data: " + str(t))
            # data = input(' -> ')
            if int(recemsgsplit[0]) != membership:
                print("Membership change")
                print(str("Total Members: " + recemsgsplit[0]))
                membership = int(recemsgsplit[0])
            conn.send(data.encode())  # send data to the client
            break
    conn.close()  # close the connection


def send_check_point(ipaddr, state, last_client1_seq_no, last_client2_seq_no, no_of_messages_updated):
    host = str(ipaddr)  # as both code is running on same pc
    # host = '128.237.222.174'
    port = 6883  # socket server port number
    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the serv
    message = str(state + ' ' + last_client1_seq_no + ' ' + last_client2_seq_no + ' ' + str(no_of_messages_updated))  # take input
    print("Sending Message: " + message)
    client_socket.send(message.encode())
    time.sleep(1)
    # send message
    data = client_socket.recv(1024).decode()  # receive response        
    # print(data)
    client_socket.close()  # close the connection


def receive_check_point():
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global no_of_messages_updated
    # get the hostname
    # host = '128.237.119.43'
    host = '172.0.0.1'
    # print (host)
    port = 6883  # initiate port no above 1024
    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together
    print('Bind Complete')
    # configure how many client the server can listen simultaneously
    server_socket.listen(0)
    print('Listening.....')
    a = 1
    t = []
    while a == 1:
        conn, address = server_socket.accept()  # accept new connection
        while True:
            # receive data stream. it won't accept data packet greater than 1024 bytes
            data = conn.recv(1024).decode()
            if not data:
                a = 0
                # if data is not received break
                break
            print("from connected user: " + str(data))
            # data = input(' -> ')
            recemsg = str(data)
            recemsgsplit = recemsg.split(' ')
            state = int(recemsgsplit[0])
            last_client1_seq_no = int(recemsgsplit[1])
            last_client2_seq_no = int(recemsgsplit[2])
            no_of_messages_updated = int(recemsgsplit[3])
            a = 0
            break

    conn.close()  # close the connection
    pass


def send_logged_data():
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global membership
    global checkpoint_flag
    global membership_no
    global no_of_messages_updated
    global consistency_flag

    host = socket.gethostname()  # as both code is running on same pc
    port = 5000  # socket server port number

    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server
    f = open("logged_data.csv", "rb")
    l = f.read(1024)
    while l:
        client_socket.send(l)
        l = f.read(1024)
    f.close()
    client_socket.close()  # close the connection
    pass


def receive_logged_data():
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global membership
    global checkpoint_flag
    global membership_no
    global no_of_messages_updated
    global consistency_flag

    host = socket.gethostname()
    port = 5000  # initiate port no above 1024
    i = 1
    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together
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


def check_point_send_check():

    membercheck = []
    ipaddr = []
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global membership
    global checkpoint_flag
    global membership_no
    global no_of_messages_updated
    global consistency_flag

    with open('membership.csv') as csvDataFile:
        csvReader = csv.reader(csvDataFile)
        for row in csvReader:
            if row:
                membercheck.append(row[0])
                ipaddr.append(row[2])
    mem = int(membercheck[0])
    if mem > 1:
        if checkpoint_flag == 0:
            blocking_client()
            print("BLOCKING IMPLEMENTED: QUIESCENCE")
            checkpoint_flag = 1
            if membership_no == 1:
                time.sleep(3)
                send_check_point(ipaddr, state, last_client1_seq_no, last_client2_seq_no, no_of_messages_updated)
                time.sleep(7)
                send_logged_data()
            else:
                receive_check_point()
                time.sleep(1)
                receive_logged_data()
            unblocking_client()
            print("BLOCKING STOPPED")


def blocking_client():
    a = 1
    aa = []
    aa.append(a)
    myfile = open('replica_blocking.csv', 'w')
    with myfile:
        writer = csv.writer(myfile)
        writer.writerows([aa])
    time.sleep(3)


def unblocking_client():
    a = 0
    aa = []
    aa.append(a)
    myfile = open('replica_blocking.csv', 'w')
    with myfile:
        writer = csv.writer(myfile)
        writer.writerows([aa])
    time.sleep(3)


def send_update(client, seq_no, message):
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global membership
    global checkpoint_flag
    global membership_no
    global no_of_messages_updated
    global consistency_flag

    ipaddr= []
    with open('membership.csv') as csvDataFile:
        csvReader = csv.reader(csvDataFile)
        for row in csvReader:
            if row:
                ipaddr.append(row[2])
    host = str(ipaddr)  # as both code is running on same pc
    # host = '128.237.222.174'
    port = 6883  # socket server port number
    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the serv
    message = str(
        client + ' ' + seq_no + ' ' + message + ' ' + str(no_of_messages_updated))  # take input
    print("Sending Message: " + message)
    client_socket.send(message.encode())
    time.sleep(1)
    # send message
    data = client_socket.recv(1024).decode()  # receive response
    # print(data)
    client_socket.close()  # close the connection
    pass


def receive_update():
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global no_of_messages_updated
    # get the hostname
    # host = '128.237.119.43'
    host = '172.0.0.1'
    # print (host)
    port = 6883  # initiate port no above 1024
    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together
    print('Bind Complete')
    # configure how many client the server can listen simultaneously
    server_socket.listen(0)
    print('Listening.....')
    a = 1
    t = []
    while a == 1:
        conn, address = server_socket.accept()  # accept new connection
        while True:
            # receive data stream. it won't accept data packet greater than 1024 bytes
            data = conn.recv(1024).decode()
            if not data:
                a = 0
                # if data is not received break
                break
            print("from connected user: " + str(data))
            # data = input(' -> ')
            recemsg = str(data)
            recemsgsplit = recemsg.split(' ')
            client_id = int(recemsgsplit[0])
            client_seq_no = int(recemsgsplit[1])
            message = int(recemsgsplit[2])
            no_of_messages_updated = int(recemsgsplit[3])
            state = int(message)
            printline = str("The latest state is " + str(state))
            print(printline)
            if client_id == 'c1':
                last_client1_seq_no = int(client_seq_no)
            else:
                last_client2_seq_no = int(client_seq_no)
            a = 0
            break

    conn.close()  # close the connection
    pass


def set_total_order():
    global last_client1_seq_no
    global last_client2_seq_no
    global state
    global membership
    global checkpoint_flag
    global membership_no
    global no_of_messages_updated
    global consistency_flag

    if consistency_flag == 0:
        if membership_no == 1:
            message =[]
            seq_no = []
            client = []
            with open('logged_data.csv') as csvDataFile:
                csvReader = csv.reader(csvDataFile)
                for row in csvReader:
                    if row:
                        client.append(row[0])
                        seq_no.append(row[1])
                        message.append(row[2])
            for i in range(no_of_messages_updated,len(client)):
                send_update(client[i],seq_no[i],message[i])
                state = int(client[i])
                no_of_messages_updated = int(no_of_messages_updated) + 1
                printline = str("The latest state is " + str(state))
                print(printline)
                if client[i] == 'c1':
                    last_client1_seq_no = int(seq_no)
                else:
                    last_client2_seq_no = int(seq_no)

        else:
            receive_update()




if __name__ == '__main__':
    last_client1_seq_no = 0
    last_client2_seq_no = 0
    state = 0
    membership = 1
    checkpoint_flag = 0
    membership_no = 2
    no_of_messages_updated = 0
    consistency_flag = 0
    unblocking_client()
    while True:
        thread.start_new_thread(server_program_rm, ())
        time.sleep(1)
        thread.start_new_thread(heartbeat, ())
        thread.start_new_thread(check_point_send_check,())
        server_program_client()
        time.sleep(1)
