import socket,time
from random import randint

TCP_IP = "localhost"
TCP_PORT = 7077
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
filename = '/home/senthil/code/pyspark/fraud/data/big_file.txt'
with open(filename,'r') as f:
    next(f)
    for x in f:
        conn.send(str(x.split(',')[0]) +' 1' + '\n')
