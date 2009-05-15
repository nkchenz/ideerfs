import aio

import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', 8989))

n = aio.AIO()

io = n.read(s, 20)

for i in range(4):
    n.write(s, '12345678')

n.wait(io)


n.close()
