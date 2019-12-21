import socket
import os
import csv
from datetime import datetime
import time

GFD_PORT = "gfd_ports.csv"
REPLICATION_MANAGER_PORT = "Replication_Manager_membership.csv"
LIST_IPs = []
LIST_PORTs = []
ALIVE_IPs = []
ALIVE_PORTs = []
ALIVE_LFDs = []
ALIVE_IPs_OLD = []
ALIVE_LFDs_OLD = []
ALIVE_IPs_ORIGINAL = []
flag_all_replicas = False
change_in_membership = False

# GFD is ready when it creates gfd_ports.csv file
def gfd_ready():
    while not os.path.exists(GFD_PORT):
        pass
    print(str(datetime.now()) + ">", end=" ")
    print("GFD is up!")
    print("Number of members = 0")


# Read the list of IPs and ports from the gfd_ports.csv
def read_from_csv():
    global LIST_IPs
    global LIST_PORTs

    info = []

    with open(GFD_PORT) as csvDataFile:
        csvReader = csv.reader(csvDataFile)
        for row in csvReader:
            info.append(row)
    # info can be empty when the GFD is writing to the CSV file, do not parse it
    if len(info) != 0:
        if len(info[0]) != 0:
            LIST_IPs = info[0]
        if len(info[1]) != 0:
            LIST_PORTs = info[1]


# LFDs are ready when the GFD updates all IPs to be non-zero
def lfds_ready():
    global LIST_IPs
    lfd_ready = False
    read_from_csv()
    for IP in LIST_IPs:
        if IP != "0":
            lfd_ready = True
            break
    return lfd_ready


# Replicas are ready when the GFD updates all ports to be non-zero
def replicas_ready():
    global LIST_PORTs
    replica_ready = False
    read_from_csv()
    for port in LIST_PORTs:
        if port != "0" or port != '':
            replica_ready = True
            break
    return replica_ready


# Replicas whose port numbers are non-zero are alive
def update_alive_replica():
    global ALIVE_IPs
    global ALIVE_PORTs
    global LIST_IPs
    global LIST_PORTs
    global ALIVE_IPs_OLD

    ALIVE_IPs = []
    ALIVE_PORTs = []

    for index in range(len(LIST_PORTs)):
        if LIST_PORTs[index] != "0":
            #print("check--->", LIST_PORTs[index])
            ALIVE_IPs.append(LIST_IPs[index])
            ALIVE_PORTs.append(LIST_PORTs[index])


# Display IPs of active replicas
def print_membership():
    global ALIVE_IPs
    print("Number of members: " + str(len(ALIVE_IPs)) + "|", end=" ")
    print("Membership: ", end=" ")
    for IP in ALIVE_IPs:
        print(IP, end=" ")
    print(" ")


# Display IPs of active replicas
def update_alive_lfd():
    global ALIVE_LFDs
    global ALIVE_LFDs_OLD

    ALIVE_LFDs = []

    for index in range(len(LIST_IPs)):
        if LIST_IPs[index] != "0":
            ALIVE_LFDs.append(LIST_IPs[index])


# Display IPs of active LFDs
def print_alive_lfd():
    global ALIVE_LFDs
    print("Number of LFDs up: " + str(len(ALIVE_LFDs)) + "|", end=" ")
    print("LFDs: ", end=" ")
    for IP in ALIVE_LFDs:
        print(IP, end=" ")
    print(" ")


# At startup of RM, check if GFDs and Replicas are ready and update membership
# Send membership to active replicas
def startup():
    while not os.path.exists("gfd_startup.txt"):
        pass
    f_creategfd = open("gfd_startup.txt")
    a = f_creategfd.read()

    if(a=="1"):
        print("GFD is up!")

    if os.path.exists(REPLICATION_MANAGER_PORT):
        os.remove(REPLICATION_MANAGER_PORT)

    f_create = open(REPLICATION_MANAGER_PORT, 'w+')
    f_create.close()
    print(str(datetime.now()) + ">", end=" ")
    print("Replication manager is ready!")
    gfd_ready()
    while not lfds_ready():
        pass
    print(str(datetime.now()) + ">", end=" ")
    #print("LFD/s are ready!")
    update_alive_lfd()
    print_alive_lfd()
    while not replicas_ready():
        pass
    print(str(datetime.now()) + ">", end=" ")
    #print("Replica/s are ready!")
    update_alive_replica()
    write_to_csv()
    print_membership()
    send_membership_to_replicas()


# Check for change in active LFDs
def check_lfd_change():
    global ALIVE_LFDs
    global ALIVE_LFDs_OLD
    ALIVE_LFDs_OLD = ALIVE_LFDs.copy()
    update_alive_lfd()
    if not (ALIVE_LFDs_OLD == ALIVE_LFDs):
        print(str(datetime.now()) + ">", end=" ")
        print("Change in active LFDs: ", end=" ")
        print_alive_lfd()


# Check for change in active Replicas and send new membership to replicas
def check_membership_change():
    global ALIVE_IPs
    global ALIVE_IPs_OLD
    global LIST_IPs
    global ALIVE_IPs_ORIGINAL
    global flag_all_replicas
    global primary
    global change_in_membership
    
    change_in_membership = False
    ALIVE_IPs_OLD = ALIVE_IPs.copy()
    if len(ALIVE_IPs) == 3 and flag_all_replicas==False:
        flag_all_replicas = True
        ALIVE_IPs_ORIGINAL.append([ALIVE_IPs[0],"0","0"])
        ALIVE_IPs_ORIGINAL.append(["0", ALIVE_IPs[1],"0"])
        ALIVE_IPs_ORIGINAL.append(["0", "0",ALIVE_IPs[2]])
        ALIVE_IPs_ORIGINAL.append([ALIVE_IPs[0], ALIVE_IPs[1], "0"])
        ALIVE_IPs_ORIGINAL.append(["0", ALIVE_IPs[1], ALIVE_IPs[2]])
        ALIVE_IPs_ORIGINAL.append([ALIVE_IPs[0], "0", ALIVE_IPs[2]])
        ALIVE_IPs_ORIGINAL.append(ALIVE_IPs.copy())
    update_alive_replica()
    if not (ALIVE_IPs_OLD == ALIVE_IPs):
        change_in_membership = True
        print(str(datetime.now()) + ">", end=" ")
        print("Change in membership: ", end=" ")
        print_membership()
        write_to_csv()
        send_membership_to_replicas()
        
        


def write_to_csv():
    global ALIVE_IPs
    global ALIVE_PORTs
    myfile = open(REPLICATION_MANAGER_PORT, 'w', newline='')
    with myfile:
        writer = csv.writer(myfile)
        writer.writerows([ALIVE_IPs, ALIVE_PORTs])


# Send membership to all active replicas: <Number of replicas> <Replica1 IP> <Replica2 IP>...
# If there is a single active replica, then send 0 in place of <Replica2 IP>
def send_membership_to_replicas():
    global ALIVE_IPs
    global ALIVE_PORTs
    message = pack_message()
    for i in range(len(ALIVE_IPs)):
        host = ALIVE_IPs[i]
        port = int(ALIVE_PORTs[i]) - 10
        client_socket = socket.socket()  # instantiate
        client_socket.connect((host, port))  # connect to the server
        client_socket.send(message.encode())  # send message
        time.sleep(1)
        data = client_socket.recv(1024).decode()  # receive response
        client_socket.close()  # close the connection


def pack_message():
    global ALIVE_IPs
    global ALIVE_IPs_ORIGINAL
    global flag_all_replicas
    global primary

    replica1_flag = False
    replica2_flag = False
    replica3_flag = False

    message = str(len(ALIVE_IPs)) + " "
    if flag_all_replicas:
        for IP in ALIVE_IPs:
            if IP == ALIVE_IPs_ORIGINAL[0][0]:
                replica1_flag = True
            elif IP == ALIVE_IPs_ORIGINAL[1][1]:
                replica2_flag = True
            elif IP == ALIVE_IPs_ORIGINAL[2][2]:
                replica3_flag = True
        
        correct_list = [] 
        if len(ALIVE_IPs)==1:
            if replica1_flag:
                correct_list = ALIVE_IPs_ORIGINAL[0]
            elif replica2_flag:
                correct_list = ALIVE_IPs_ORIGINAL[1]
            elif replica3_flag:
                correct_list = ALIVE_IPs_ORIGINAL[2]

        elif len(ALIVE_IPs)==2:
            if replica1_flag and replica2_flag:
                correct_list = ALIVE_IPs_ORIGINAL[3]
            if replica2_flag and replica3_flag:
                correct_list = ALIVE_IPs_ORIGINAL[4]
            if replica1_flag and replica3_flag:
                correct_list = ALIVE_IPs_ORIGINAL[5]

        elif len(ALIVE_IPs)==3:
            correct_list = ALIVE_IPs_ORIGINAL[6]
            
        if(correct_list[primary]=="0"):
            for index in range(len(correct_list)):
                if correct_list[index] != "0":
                    primary = index
                    break
                    
        for IP in correct_list:
            message = message + IP +" "

    else:
        for IP in ALIVE_IPs:
            message = message + IP + " "
        if len(ALIVE_IPs) == 1:
            message = message + "0 " + "0" + " "
        elif len(ALIVE_IPs) == 2:
            message = message + "0" " "
    
    message = message + str(primary)
    
    return message


def run():
    while True:
        read_from_csv()
        check_lfd_change()
        check_membership_change()


def main():
    global primary
    primary = 0
    startup()
    run()


main()

# for IP in LIST_IPs:
# print(IP, end=" ")
# print(" ")
"""        for IP in LIST_IPs:
            print(IP, end=" ")
        print(" ")
        for ports in LIST_PORTs:
            print(ports, end=" ")
        print(" ")"""
