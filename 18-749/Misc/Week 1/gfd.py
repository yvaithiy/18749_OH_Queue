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
	f = open("foo.txt", "w+")
	f.write("1")
	f.close()


#get start signals from all lfds
def start2():
	ip_addr = '128.237.216.218'
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
		print(socket.gethostbyname(socket.gethostname()))
		print("waiting for connection from lfd")
		conn, addr = server.accept()
		print('Connection address:' + str(addr))
		data = conn.recv(1024)
		try:
			data = pickle.loads(data)
		except:
			print("LFD Crashed")
			break
		print("received data:" + str(data))
		if data["type"] == "INIT":
			if data["replica_ip"] not in replica_ips:
				replica_ips.append(data["replica_ip"])
				ports.append(data["replica_port"])
				lfd_status.append("1")
				replica_status.append("1")
	lfd_ips = replica_ips

	with open('gfd_port.csv', 'w+') as writeFile:
		writer = csv.writer(writeFile)
		writer.writerow(lfd_ips)
		writer.writerow(lfd_status)
		writer.writerow(replica_ips)
		writer.writerow(replica_status)
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
			with open('gfd_port.csv', 'w') as writeFile:
				print("writing")
				writer = csv.writer(writeFile)
				writer.writerow(ips)
				writer.writerow("1")
				writer.writerow(ips)
				#need to account for num_replicas here
				if(data["replica_status"] == "True"):
					writer.writerow("1")
				else:
					writer.writerow("0")
				writer.writerow(ports)
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
