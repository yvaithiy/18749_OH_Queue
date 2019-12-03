import csv
import time
import random

GFD_PORT = "gfd_ports.csv"


def change():
    ALIVE_IPs = random.sample(range(100), 2)
    ALIVE_PORTs = random.sample(range(100), 2)
    myfile = open(GFD_PORT, 'w', newline='')
    with myfile:
        writer = csv.writer(myfile)
        writer.writerows([ALIVE_IPs,ALIVE_PORTs])


def main():
    time.sleep(10)
    while True:
        change()
        print("File written")
        time.sleep(5)

main()
