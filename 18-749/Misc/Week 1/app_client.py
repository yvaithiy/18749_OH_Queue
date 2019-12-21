
import time
import socket
import json

RM_IP = "localhost"
RM_port = 999
C_port = 998

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
    message_dict = json.loads(message_str)
    return message_dict

def run():
    while True:
        user_input = input("user cmd>")
        # sock_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # sock_send.connect((RM_IP, RM_port))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Bind the socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(1)
        sock.bind(("localhost", C_port))
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

        membership = {
            "type": "MEM_RESPONSE",
            "replicas": [
                {
                    "ip": "localhost",
                    "port": 5000,
                    "status": 1
                },
                {
                    "ip": "localhost",
                    "port": 5000,
                    "status": 1
                },
                {
                    "ip": "localhost",
                    "port": 5000,
                    "status": 1
                }
            ]
        }

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
        for r in membership["replicas"]:
            if r["status"] == 1:
                sock_sendr = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock_sendr.connect((r["ip"], r["port"]))
                msg = json.dumps(req)
                sock_sendr.sendall((msg.encode("utf-8")))
            
        if cmd[0] == "r":
            while True:
                try:
                    sock_recv, _ = sock.accept()
                except socket.timeout:
                    continue
                value = get_single_msg(sock_recv)
            print("The value at the top of the stack is " + str(value["value"]))

if __name__ == "__main__":
    run()