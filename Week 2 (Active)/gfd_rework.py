import socket
import sys
import json
import csv
import timeit
import pickle
import select
import datetime
import time
from _thread import *
import threading

active_conns = []
addresses = []
lock = threading.Lock()

def start():
    #tell replication manager we're on
    f = open("gfd_startup.txt", "w+")
    f.write("1")
    f.close()
    t = []
    with open('gfd_ports.csv', 'w+') as writeFile:
        writer = csv.writer(writeFile, delimiter=',')
        writer.writerow(["0","0"])
        writer.writerow(["0","0"])


#Heartbeat the known lfds and timeout if no response
def heartbeat(client, addr, port, thread_id):
    timeout = 2.0
    global lock

    while 1:
        #print("waiting for data")
        data = client.recv(1024)
        print("recv did not hang")
        if(data):
            time = datetime.datetime.now().time()
            time_min = time.minute
            time_sec = time.second
            curr_time = time_min*60+time_sec
            #print("Heartbeat Received")
            data = pickle.loads(data)
            #print(data)

            #Update csv based on data received in heartbeat message
            contents = []
            lock.acquire()
            with open('gfd_ports.csv', 'r') as readfile:
                csvreader = csv.reader(readfile, delimiter=',')
                for row in csvreader:
                    contents.append(row)
                with open('gfd_ports.csv', 'w') as writeFile:
                    writer = csv.writer(writeFile)
                    #print("writing")
                    #set port to 0 if lfd says the replica is down
                    if(data["replica_status"] is not True):
                        writer.writerow(contents[0])
                        contents[1][thread_id] = 0
                        writer.writerow(contents[1])
                    #reset port to original value if replica is coming back up from downtime
                    else:
                        writer.writerow(contents[0])
                        contents[1][thread_id] = port
                        writer.writerow(contents[1])
            lock.release()
        else:
            time2 = datetime.datetime.now().time()
            time_min2 = time2.minute
            time_sec2 = time2.second
            wait_time = time_min2*60+time_sec2
            if(wait_time - curr_time > timeout):
                contents = []
                #If timeout occured update csv to contain 0 for ip and port for the lfd that went down
                lock.acquire()
                with open('gfd_ports.csv', 'r') as readfile2:
                    csvreader = csv.reader(readfile2, delimiter=',')
                    for row in csvreader:
                        contents.append(row)
                    with open('gfd_ports.csv', 'w') as writeFile2:
                        writer = csv.writer(writeFile2)
                        #print("writing")
                        if(data["replica_status"] is not True):
                            contents[0][thread_id] = 0
                            contents[1][thread_id] = 0
                            writer.writerow(contents[0])
                            writer.writerow(contents[1])
                lock.release()
                print("connection timeout")
                break


#Wait for lfds to tell us the replicas are up
def lfd_init(client, addr, thread_id):
    global lock

    print("in thread " + str(thread_id))
    print(addr)
    contents = [[]]
    data = client.recv(1024)
    # try:
    #print("before loads"+str(data))
    data = pickle.loads(data)
    lock.acquire()
    #read and update contents of csv
    with open('gfd_ports.csv', 'r') as readfile:
        csvreader = csv.reader(readfile, delimiter=',')
        #print(readfile)
        for row in csvreader:
            temp_cont = row
            contents.append(temp_cont)
    contents.pop(0)
    #print(contents)
    contents[0][thread_id] = data["replica_ip"]
    contents[1][thread_id] = data["replica_port"]
    #print(contents)
    with open('gfd_ports.csv', 'w+') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerow(contents[0])
        writer.writerow(contents[1])
    lock.release()
    # except:
    #     print("bad data1: " + str(data))

    #Wait for non-zero port number to know replica is up
    #print("received data:" + str(data))
    while(data["replica_port"] == '0'):
        #print("still waiting")
        data = client.recv(1024)
        try:
            data = pickle.loads(data)
        except:
            print("bad data2: " + str(data))
            break
    print("replica is up and running")
    #Update csv with non-zero port number
    lock.acquire()
    with open('gfd_ports.csv', 'r') as readfile2:
        csvreader = csv.reader(readfile2, delimiter=',')
        for row in csvreader:
            contents.append(row)
    contents[0][thread_id] = data["replica_ip"]
    contents[1][thread_id] = data["replica_port"]
    with open('gfd_ports.csv', 'w+') as writeFile2:
        writer = csv.writer(writeFile2)
        writer.writerow(contents[0])
        writer.writerow(contents[1])
    lock.release()

    heartbeat(client, addr, data["replica_port"], thread_id)


def main():
    print("main")
    global active_conns
    global addresses
    start()
    ip_addr = '128.237.168.193'

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((ip_addr, 5005))
    server.listen(1)

    #accept any number of connections
    while True:
        c, addr = server.accept()
        print(addr)
        if addr not in active_conns:
            active_conns.append(addr)
        #The address of the connection is used to identify threads for indexing the csv
        #This should remain consistent throughout timeouts of the lfds
        t1 = threading.Thread(target=lfd_init, args=(c, addr, active_conns.index(addr)))
        t1.start()

if __name__== "__main__":
    main()
