import time
import socket
import json
import csv

RM_IP = "0.0.0.0"
RM_port = 999
C_port = 998
REPLICATION_MANAGER_PORT = "gft_port1.csv"


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
    while True:
        user_input = input("user cmd>")
        # sock_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # sock_send.connect((RM_IP, RM_port))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Bind the socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(1)
        sock.bind(("0.0.0.0", C_port))
        sock.listen(5)

        # membership_req = {
        #     "type": "membership"
        # }
        # msg = json.dumps(membership_req)
        # sock_send.sendall((msg.encode("utf-8")))

        # while True:
        #     try:
        #         sock_recv, _ = sock.accept()
        #     except socket.timeout:
        #         continue
        #     member = get_single_msg(sock_recv)

        f = open(REPLICATION_MANAGER_PORT, "r")
        csv_reader1 = csv.reader(f, delimiter=',')

        replica_ips = []
        replica_ports = []
        replica_status = []
        # alive_replica_ips = ['128.237.140.23']
        # alive_replica_ports = [6883]
        row_num = 0
        for row in csv_reader1:
            if row_num == 2:
                replica_ips = row
            elif row_num == 3:
                replica_status = row
            elif row_num == 4:
                replica_ports = row
            row_num = row_num + 1
        f.close()
        print(replica_ips)
        print(replica_ports)
        print(replica_status)

        cmd = user_input.split(' ')
        if cmd[0] == "r":
            req = {
                "type": "read"
            }
        elif cmd[0] == "w":
            req = {
                "type": "write",
                "value": cmd[1]
            }
        for i in range(len(replica_ips)):
            if replica_status[i] == '1':
                sock_sendr = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock_sendr.connect((replica_ips[i], int(replica_ports[i])))
                msg = json.dumps(req)
                sock_sendr.sendall((msg.encode("utf-8")))
                sock_sendr.close()

        if cmd[0] == "r":
            while True:
                try:
                    sock_recv, _ = sock.accept()
                except socket.timeout:
                    continue
                value = get_single_msg(sock_recv)
                break
            print("The value at the top of the stack is " + str(value))



if __name__ == "__main__":
    run()