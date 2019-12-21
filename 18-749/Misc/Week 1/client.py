import sys
import socket
import time
import csv
import json

def client_program():
    #host = '192.168.1.38'  # as both code is running on same pc
    host = '172.0.0.1'
    port = 998 # socket server port number

    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server
    a= []
    with open('stack.csv') as csvDataFile:
        csvReader = csv.reader(csvDataFile)
        for row in csvReader:
            a.append(row)
    message = str(a[0][0])  # take input
    print("Sending Message: " + message)
    #message = {
                 #"type": "READ"
              #}
    #message = json.dumps(message)
    client_socket.send(message.encode())
    time.sleep(1)  
    # send message
    #data = client_socket.recv(1024).decode()  # receive response        
    #print(data)
    #client_socket.close()  # close the connection


if __name__ == '__main__':
    client_program()
