import socket
import csv
import pickle
import datetime
import time

REPLICA_PORT = "replica_port.csv"
REPLICA_HEARTBEAT = "replica_heartbeat.csv"
GFD_IP = '128.237.166.120'

LFD_PORT = 5005
BUFFER_SIZE = 1024
THRESHOLD = 10
GFD_INTERVAL = 1

# print(socket.gethostbyname(socket.gethostname()))

LFD_IP = socket.gethostbyname(socket.gethostname())

print("LFD running at IP: "+str(LFD_IP))
def start(s):
	replica_ready = False
	f= open(REPLICA_PORT,"r")
	content = f.read()
	while (not content):
		content = "0"
		message = {}
		message["type"] = "INIT"
		message["replica_ip"] = str(LFD_IP)
		message["replica_port"] = content

		s.send(pickle.dumps(message))
		time.sleep(GFD_INTERVAL)
		content = f.read()
	
	print("Replica Running...")
	f.close()
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
		last_time = datetime.datetime.strptime(last_time, '%Y-%m-%d %H:%M:%S.%f')
		last_minute = last_time.minute
		last_second = last_time.second
		last_time  = last_minute*60 + last_second
		if (abs(current_time-last_time) > THRESHOLD):
			alive = False
			print("Replica Died!")
		else:
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

