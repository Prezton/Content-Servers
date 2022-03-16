import socket, sys

BUFSIZE = 1024
PORT = 18346
SERVER = socket.gethostbyname("ece007.ece.local.cmu.edu")
# SERVER = "127.0.0.1"
ADDR = (str(SERVER), PORT)
client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
print(ADDR)

while True:
    msg = input("Client input: ")
    client.sendto(msg.encode(), ADDR)
    if (msg == "DISCONNECT"):
        print("BREAK")
        break
    # print("Server: ", client.recvfrom(BUFSIZE)[1])
