import csv
import time
import random

GFD_PORT = "gfd_ports.csv"


def change1():
    #ALIVE_IPs = random.sample(range(100), 2)
    #ALIVE_PORTs = random.sample(range(100), 2)
    ALIVE_IPs = ["128.237.195.202", "128.237.195.202"]
    ALIVE_PORTs = [6897,6897]
    myfile = open(GFD_PORT, 'w', newline='')
    with myfile:
        writer = csv.writer(myfile)
        writer.writerows([ALIVE_IPs,ALIVE_PORTs])


def change2():
    #ALIVE_IPs = random.sample(range(100), 2)
    #ALIVE_PORTs = random.sample(range(100), 2)
    ALIVE_IPs = ["128.237.195.202"]
    ALIVE_PORTs = [6897]
    myfile = open(GFD_PORT, 'w', newline='')
    with myfile:
        writer = csv.writer(myfile)
        writer.writerows([ALIVE_IPs,ALIVE_PORTs])


def main():
    delay = 10
    #while True:
    time.sleep(delay)
    change1()
    print("File written")
    time.sleep(delay)
    change2()


main()
