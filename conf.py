
"""Global configurations"""

NAME = 'ideerfs'
VERSION = '0.1'
CODE_NAME = 'TheBestOfYouth'
REQUEST_HEADER_SIZE = 1024 # Message size, without bulk data
BUFFER_SIZE = 1024 * 32 # 32k read and write buffer for network traffic, change this for better performence


LISTEN_PORT = 51984
LISTEN_ADDRESS = 'localhost'               
MAX_WAITING_CLIENTS = 128 # backlog for listen?
WELCOME_MESSAGE = '%s-%s %s' % (NAME, VERSION, CODE_NAME)

# Make sure you have write permission of these files, abosolute path please
PID_FILE = NAME + '.pid'
LOG_FILE = NAME + '.log'


