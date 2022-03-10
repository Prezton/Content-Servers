import socket, sys

BUFSIZE = 1024
PORT = 18346
SERVER = "127.0.0.1"
ADDR = (SERVER, PORT)
client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


while True:
    msg = input("Client input: ")
    client.sendto(msg.encode(), ADDR)
    if (msg == "DISCONNECT"):
        print("BREAK")
        break
    print("Server: ", client.recvfrom(BUFSIZE))
