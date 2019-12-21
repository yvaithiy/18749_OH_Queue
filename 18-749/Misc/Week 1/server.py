import socket
from datetime import datetime
import csv
def server_program():
    # get the hostname
    #host = '192.168.1.207'
    host= '127.0.0.1'
    print (host)
    port = 6883  # initiate port no above 1024

    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together
    print ('Bind Complete')
    # configure how many client the server can listen simultaneously
    server_socket.listen(0)
    print ('Listening.....')
    a=1   
    t = []
    while a==1:
        conn, address = server_socket.accept()  # accept new connection
        while True: 
            # receive data stream. it won't accept data packet greater than 1024 bytes
            data = conn.recv(1024).decode()
            if not data:
                a=0
                # if data is not received break
                break
            print("from connected user: " + str(data))
            t.append(str(data))
            myfile=open('stack.csv','w')
            with myfile:
                writer=csv.writer(myfile)
                writer.writerows([t])
            #data = input(' -> ')
            conn.send(data.encode())  # send data to the client

    conn.close()  # close the connection
    
if __name__ == '__main__':
    server_program()
