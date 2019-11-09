import socket
import sys
import json
import csv
import timeit
import pickle
import select


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


def start1():
	#tell replication manager we're on?
	f = open("foo.txt", "w+")
	f.write("1")
	f.close()


#get start signals from all lfds
def start2():
	num_replicas = 1
	global server
	#allow socket to be seen to outside world
	server.bind((socket.gethostbyname(socket.gethostname()), 5005))
	server.listen(1)
	replica_ips = []
	ports = []
	lfd_status = []
	replica_status = []
	while len(replica_ips) < num_replicas:
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
	global server
	start1()
	ips, ports, conn = start2()
	timeout = 2.0
	while 1:
		server.setblocking(0)
		ready = select.select([server], [], [], timeout)
		if ready[0]:
			data = conn.recv(1024)
			try:
				data = pickle.loads(data)
				#might want to only do this if data is unique
				with open('gfd_port.csv', 'w') as writeFile:
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
			except:
				pass
		else:
			print("Connection Timeout")



if __name__== "__main__":
    main()
