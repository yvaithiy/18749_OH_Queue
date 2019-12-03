import datetime
import csv
import socket
import os
import time
import json
import _thread as thread
##################################################################################
#################### STORING TIME IN A CSV FILE:##################################
def heartbeat():
	#print("called")
	while True:
		a=str(datetime.datetime.now())
		aa = []
		aa.append(a)
		myfile=open('REPLICA_HEARTBEAT.csv','w')
		with myfile:
			writer=csv.writer(myfile)
			writer.writerows([aa])
		time.sleep(1)

##################################################################################
############### RUNNING SERVER CODE TO CHECK R/W:#################################
def server_program():
	# get the hostname
	#host = '192.168.1.207'
	host= '172.0.0.1'
	#print (host)
	port = 6883  # initiate port no above 1024
	x = []
	x.append(port)
	myfile=open('REPLICA_PORT.csv','w')
	with myfile:
		writer=csv.writer(myfile)
		writer.writerows([x])
	print ('Port Stored')
	server_socket = socket.socket()  # get instance
	# look closely. The bind() function takes tuple as argument
	server_socket.bind((host, port))  # bind host address and port together
	print ('Bind Complete')
	# configure how many client the server can listen simultaneously
	server_socket.listen(0)
	print ('Listening.....')
	a=1  
	t=[] 
	while a==1:
		conn, address = server_socket.accept()  # accept new connection
		while True:
			# receive data stream. it won't accept data packet greater than 1024 bytes
			data = conn.recv(1024).decode()
			if not data:
				a=0
				# if data is not received break
				break
			print("from connected user: " + str(data))
			#data = input(' -> ')
			recemsg = json.loads(data)
			conn.send(data.encode())  # send data to the client
			##print(final[0])
			if(recemsg["type"] == 'read'):
				os.system('python client.py')
				#print("data sent")
				a = 0
				break
			elif(recemsg["type"] == 'write'):
				t.append(recemsg['value'])
				myfile=open('stack.csv','w')
				with myfile:
					writer=csv.writer(myfile)
					writer.writerows([t])
				print("Writing Data: " + t)
				a = 0
				break
				
	conn.close()  # close the connection
	print("Connection Closed")
	
########################################################################################
############################ MAIN PROGRAM: #############################################
if __name__ == '__main__':
	while(True):
		thread.start_new_thread(heartbeat,())
		server_program()
		time.sleep(1)