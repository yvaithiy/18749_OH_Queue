import socket
import sys
import json
import csv
import timeit
import pickle
import select
import datetime
import time


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


def start1():
    #tell replication manager we're on?
    f = open("gfd_start.txt", "w+")
    f.write("1")
    f.close()
    with open('gfd_ports.csv', 'w+') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerow(["0"])
        writer.writerow(["0"])

#get start signals from all lfds
def start2():
    ip_addr = '172.26.217.19'
    num_replicas = 1
    global server
    print("in start2")
    #allow socket to be seen to outside world
    server.bind((ip_addr, 5005))
    server.listen(1)
    replica_ips = []
    ports = []
    lfd_status = []
    replica_status = []
    while len(replica_ips) < num_replicas:
        #print(socket.gethostbyname(socket.gethostname()))
        print("waiting for connection from lfd")
        print("replica_ips: " + str(replica_ips))
        conn, addr = server.accept()
        print('Connection address:' + str(addr))
        data = conn.recv(1024)
        try:
            data = pickle.loads(data)
        except:
            print("LFD Crashed")
            print("bad data: " + str(data))
            break
        print("received data:" + str(data))

        with open('gfd_ports.csv', 'w+') as writeFile1:
            writer = csv.writer(writeFile1)
            writer.writerow([data["replica_ip"]])
            print(ports)
            writer.writerow(["0"])

        #only want to append if replica port is not 0
        while(data["replica_port"] == '0'):
            print("still waiting")
            data = conn.recv(1024)
            try:
                data = pickle.loads(data)
            except:
                print("LFD Crashed")
                print("bad data: " + str(data))
                break

        if (data["type"] == "INIT"):
            if data["replica_ip"] not in replica_ips:
                replica_ips.append(data["replica_ip"])
                ports.append(data["replica_port"])

    lfd_ips = replica_ips

    with open('gfd_ports.csv', 'w+') as writeFile2:
        writer = csv.writer(writeFile2)
        writer.writerow(replica_ips)
        print(ports)
        writer.writerow(ports)

    return replica_ips, ports, conn


#get heartbeats and data from lfds
def main():
    print("main")
    global server
    start1()
    ips, ports, conn = start2()
    timeout = 2.0

    while 1:
        print("waiting for data in main")
        #server.setblocking(0)
        #ready = select.select([server], [], [], timeout)
        data = conn.recv(1024)
        print("recv did not hang")
        #sock.settimeout(None)
        if(data):
            time = datetime.datetime.now().time()
            time_min = time.minute
            time_sec = time.second
            curr_time = time_min*60+time_sec
            print("Heartbeat Received")
            data = pickle.loads(data)
            print(data)

            #might want to only do this if data is unique
            with open('gfd_ports.csv', 'w') as writeFile:
                print("writing")
                writer = csv.writer(writeFile)
                writer.writerow(ips)
                if(data["replica_status"] == True):
                    writer.writerow(ports)
                else:
                    #if replica is down, write 0 to the port
                    writer.writerow('0')

        else:
            time2 = datetime.datetime.now().time()
            time_min2 = time2.minute
            time_sec2 = time2.second
            wait_time = time_min2*60+time_sec2
            if(wait_time - curr_time > timeout):
                print("connection timeout")
                break





if __name__== "__main__":
    main()
