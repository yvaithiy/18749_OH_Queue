import socket
import csv
import pickle
import datetime
import time

REPLICA_PORT = "replica_port.csv"
REPLICA_HEARTBEAT = "replica_heartbeat.csv"
GFD_IP = '128.237.164.94'

LFD_PORT = 5005
BUFFER_SIZE = 1024
THRESHOLD = 5
GFD_INTERVAL = 1

# print(socket.gethostbyname(socket.gethostname()))

#LFD_IP = socket.gethostbyname(socket.gethostname())
LFD_IP = '128.237.142.177'

print("LFD running at IP: "+str(LFD_IP))
def start(s):
	replica_ready = False
	f= open(REPLICA_PORT,"w")
	f.write("0")
	f.close()

	f = open(REPLICA_PORT,"r")
	content = f.read()
	f.close()
	while (not content or content=="0"):
		content = "0"
		message = {}
		message["type"] = "INIT"
		message["replica_ip"] = str(LFD_IP)
		message["replica_port"] = content

		s.send(pickle.dumps(message))
		time.sleep(GFD_INTERVAL)
		f = open(REPLICA_PORT,"r")
		content = f.read()
		f.close()
	
	print("Replica Running...")
	content = content.strip()
	message = {}
	message["type"] = "INIT"
	message["replica_ip"] = str(LFD_IP)
	message["replica_port"] = content
	print(content)
	s.send(pickle.dumps(message))
	return

def run(s):
	alive = True
	while True:
		message ={}
		d_time = datetime.datetime.now().time()
		minute = d_time.minute
		second = d_time.second
		current_time = minute*60+second
		f = open(REPLICA_HEARTBEAT, "r")
		last_time = f.read().strip()
		if last_time and len(last_time) > 1:
			noTimeFound = 0
			last_time = datetime.datetime.strptime(last_time, '%Y-%m-%d %H:%M:%S.%f')
			last_minute = last_time.minute
			last_second = last_time.second
			last_time  = last_minute*60 + last_second
			if (abs(current_time-last_time) > THRESHOLD):
				alive = False
				print("Replica Died!")
			else:
				if alive == False:
					print("Replica Alive Again! Case 1")
				alive = True
		else:
			noTimeFound += 1		
			if noTimeFound > 5:
				alive = False
				print("Time not found enough")
			else:
				if alive == False:
					print("Replica Alive Again! Case 2")
				alive = True	

		message["type"] = "HEARTBEAT"
		message["replica_status"] = alive
		s.send(pickle.dumps(message))
		time.sleep(GFD_INTERVAL)

	return

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print("Waiting to connect")
s.connect((GFD_IP, LFD_PORT))

start(s)
time.sleep(GFD_INTERVAL)
run(s)


s.close()

