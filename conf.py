
IDEERFS_PORT = 51984

REQUEST_HEADER_SIZE = 1024 # Message size, without bulk data

BUFFER_SIZE = 1024 * 32 # 32k read and write buffer for network traffic, change this for better performence



LISTEN_ADDRESS = 'localhost'               
MAX_WAITING_CLIENTS = 128 # backlog for listen?
WELCOME = 'Welcome to ideerfs, Friends!\n'

# Make sure you have write permission of these files, abosolute path please
PID_FILE = 'ideerfs.pid'
LOG_FILE = 'ideerfs.log'


