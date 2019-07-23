import psycopg2
import socket
import time
import struct
import datetime
import sys

# listen to all available interfaces
socket_host = '0.0.0.0'  # Symbolic name meaning all available interfaces
socket_port = 3387      # Arbitrary non-privileged port
socket_timeout = 2


def ListenSocket():
    global socket_con, conn, addr
    try:
        socket_con = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print 'Socket created'

        socket_con.bind((socket_host, socket_port))
        print 'Socket bind to ', socket_host, socket_port

        socket_con.listen(1)
        conn, addr = socket_con.accept()
        print('Connected by', addr)
    except:
        print 'fail to connect socket'
        
ListenSocket()
try:
   psql_con = psycopg2.connect(
        user        = "postgres",
        password    = "postgres",
        host        = '220.135.143.199',
        port        = "5432",
        database    = "accelerometer"
    )
   cursor = psql_con.cursor()

except (Exception, psycopg2.Error) as error :
    if(psql_con):
        print("Failed to connect postgres server", error)



while True:
    try:
        received = datetime.datetime.fromtimestamp(time.time())
        data = conn.recv(1224)

        if not data: 
            time.sleep(socket_timeout)
            print 'socket timeout'
            continue

        captured        = data[0:4]
        capturedFrac    = data[4:6]
        numRecords      = data[6:8]
        framingError    = struct.unpack('>B',data[8])[0]
        reserved        = data[9:23]
        checksum        = data[23]
        raw             = data[24:1224]
        
        int_time = struct.unpack('>I', captured)[0]
        date = datetime.datetime.fromtimestamp(float(int_time))
        print 'recieved',  date

       
        cursor.execute(
            """ INSERT INTO raw (source, received, captured, err_frame, data_len, data) VALUES ('%s','%s','%s','%s','%s',%s)""" 
            % (addr[0], received, date, framingError, 1200, psycopg2.Binary(raw))
            )
        psql_con.commit()
        count = cursor.rowcount
        print (count, "Record inserted successfully into raw table")

    except socket.error:
        print "Error Occured."
        break

conn.close()
#closing database connection.
if(connection):
    cursor.close()
    psql_con.close()
    print("PostgreSQL connection is closed")
