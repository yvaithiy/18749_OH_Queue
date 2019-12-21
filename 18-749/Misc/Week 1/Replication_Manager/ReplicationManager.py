import os
import csv
from datetime import datetime

GFD_PORT = "gfd_ports.csv"
REPLICATION_MANAGER_PORT = "Replication_Manager_membership.csv"
LIST_IPs = []
LIST_PORTs = []
ALIVE_IPs = []
ALIVE_PORTs = []
ALIVE_LFDs = []
ALIVE_IPs_OLD = []
ALIVE_LFDs_OLD = []


def gfd_ready():
    while not os.path.exists(GFD_PORT):
        pass
    print(str(datetime.now()) + ">",end=" ")
    print("GFD is up!")


def read_from_csv():
    global LIST_IPs
    global LIST_PORTs

    info = []

    with open(GFD_PORT) as csvDataFile:
        csvReader = csv.reader(csvDataFile)
        for row in csvReader:
            info.append(row)

    if len(info) != 0:
        LIST_IPs = info[0]
        LIST_PORTs = info[1]


def lfds_ready():
    global LIST_IPs
    lfd_ready = True
    read_from_csv()
    # Try list comprehension to filter out 0
    for IP in LIST_IPs:
        if IP == "0":
            lfd_ready = False
            break
    return lfd_ready


def replicas_ready():
    global LIST_PORTs
    replica_ready = True
    read_from_csv()
    # Try list comprehension to filter out 0
    for port in LIST_PORTs:
        if port == "0":
            replica_ready = False
            break
    return replica_ready


def update_alive_replica():
    global ALIVE_IPs
    global ALIVE_PORTs
    global LIST_IPs
    global LIST_PORTs
    global ALIVE_IPs_OLD

    ALIVE_IPs = []
    ALIVE_PORTs = []

    for index in range(len(LIST_PORTs)):
        if LIST_PORTs[index] != 0:
            ALIVE_IPs.append(LIST_IPs[index])
            ALIVE_PORTs.append(LIST_PORTs[index])

    myfile = open(REPLICATION_MANAGER_PORT, 'w', newline='')
    with myfile:
        writer = csv.writer(myfile)
        writer.writerows([ALIVE_IPs, ALIVE_PORTs])


def print_membership():
    global ALIVE_IPs
    print("Number of members: " + str(len(ALIVE_IPs)) + "|", end=" ")
    print("Membership: ", end=" ")
    for IP in ALIVE_IPs:
        print(IP, end=" ")
    print(" ")


def update_alive_lfd():
    global ALIVE_LFDs
    global ALIVE_LFDs_OLD

    ALIVE_LFDs = []

    for index in range(len(LIST_IPs)):
        if LIST_IPs[index] != 0:
            ALIVE_LFDs.append(LIST_IPs[index])


def print_alive_lfd():
    global ALIVE_LFDs
    print("Number of LFDs up: " + str(len(ALIVE_LFDs)) + "|", end=" ")
    print("LFDs: ", end=" ")
    for IP in ALIVE_LFDs:
        print(IP, end=" ")
    print(" ")


def startup():
    gfd_ready()
    while not lfds_ready():
        pass
    print(str(datetime.now()) + ">",end=" ")
    print("LFD/s are ready!")
    update_alive_lfd()
    print_alive_lfd()
    while not replicas_ready():
        pass
    print(str(datetime.now()) + ">",end=" ")
    print("Replica/s are ready!")
    update_alive_replica()
    print_membership()


def check_lfd_change():
    global ALIVE_LFDs
    global ALIVE_LFDs_OLD
    ALIVE_LFDs_OLD = ALIVE_LFDs.copy()
    update_alive_lfd()
    if not (ALIVE_LFDs_OLD == ALIVE_LFDs):
        print(str(datetime.now()) + ">",end=" ")
        print("Change in active LFDs: ", end=" ")
        print_alive_lfd()


def check_membership_change():
    global ALIVE_IPs
    global ALIVE_IPs_OLD
    global LIST_IPs
    ALIVE_IPs_OLD = ALIVE_IPs.copy()
    update_alive_replica()
    if not (ALIVE_IPs_OLD == ALIVE_IPs):
        print(str(datetime.now()) + ">",end=" ")
        print("Change in membership: ", end=" ")
        print_membership()


def run():
    global LIST_IPs
    while True:
        read_from_csv()
        check_lfd_change()
        check_membership_change()


def main():
    startup()
    run()


main()
# for IP in ALIVE_IPs_OLD:
# print(IP)
# for port in LIST_PORTs:
# print(port)
#for IP in LIST_IPs:
#print(IP, end=" ")
#print(" ")
