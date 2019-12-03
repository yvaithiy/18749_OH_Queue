import time
import socket
import json
import csv
import random

# RM_port = 999
# C_port = 998
REPLICATION_MANAGER_PORT = "Replication_Manager_membership.csv"
TIME_INTERVAL = 2 # in seconds
CLIENT_ID = 'c1'


def get_single_msg(conn):
    """Get_single_msg."""
    message_chunks = []
    while True:
        try:
            data = conn.recv(4096)
        except socket.timeout:
            continue
        if not data:
            break
        message_chunks.append(data)
    conn.close()
    message_bytes = b''.join(message_chunks)
    message_str = message_bytes.decode("utf-8")
    # message_dict = json.loads(message_str)
    # print(message_str)
    return int(message_str)


def run():
    seq = 0
    seq2 = 0
    while True:
        # user_input = input("user cmd>")
        # # sock_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # # sock_send.connect((RM_IP, RM_port))
        # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # # Bind the socket to the server
        # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # sock.settimeout(1)
        # sock.bind(("0.0.0.0", C_port))
        # sock.listen(5)
        print("----------")
        f = open(REPLICATION_MANAGER_PORT, "r")
        csv_reader1 = csv.reader(f, delimiter=',')

        replica_ips = []
        replica_ports = []
        # replica_ips = ['128.237.119.43']
        # replica_ports = [6883]
        # replica_status = ['1']
        row_num = 0
        for row in csv_reader1:
            if row_num == 0:
                replica_ips = row
            elif row_num == 1:
                replica_ports = row
            row_num = row_num + 1
        f.close()
        print(replica_ips)
        print(replica_ports)

        # cmd = user_input.split(' ')
        # if cmd[0] == "r":
        #     req = {
        #         "type": "read"
        #     }
        # elif cmd[0] == "w":
        #     req = {
        #         "type": "write",
        #         "value": cmd[1]
        #     }
        
        if random.randint(1,100)<50:
            CLIENT_ID = 'c1'
            seq = seq + 1
            msg = CLIENT_ID + ' ' + str(seq) + ' ' + str(random.randint(1, 749))
        else:
            CLIENT_ID = 'c2'
            seq2 = seq2 + 1
            msg = CLIENT_ID + ' ' + str(seq2) + ' ' + str(random.randint(1, 749))
        
        for i in range(len(replica_ips)):
            sock_sendr = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock_sendr.connect((replica_ips[i], int(replica_ports[i])))
            except:
                continue
            sock_sendr.sendall((msg.encode("utf-8")))
            #sock_sendr.close()
            
        time.sleep(TIME_INTERVAL)

        # if cmd[0] == "r":
        #     while True:
        #         try:
        #             sock_recv, _ = sock.accept()
        #         except socket.timeout:
        #             continue
        #         value = get_single_msg(sock_recv)
        #         break
        #     print("The value at the top of the stack is " + str(value))



if __name__ == "__main__":
    run()