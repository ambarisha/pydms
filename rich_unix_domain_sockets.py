import socket
from errno import EINTR
from json import loads, dumps
from os.path import abspath

MAX_MSG_LEN = 2048

class RichUnixDomainSocket:
    # returns (0, None) on success
    # returns (-1, errdesc) on failure
    def init(self, sock = None):
        if sock:
            self.sock = sock
        else:
            try:
                self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            except socket.error as se:
                return (-1, se[1])
        return (0, None)
 
    # returns (0, None) on success
    # returns (-1, errdesc) on failure
    def send_dict(self, dictionary):
        payload = dumps(dictionary)
        try:
            self.sock.send(payload)
        except socket.error as se:
            return -1, se[1] 
        return 0, None

    # returns (0, dictionary) on success
    # returns (1, None) on signal interruption
    # returns (2, errdesc) on invalid message
    # returns (-1, errdesc) on other errors
    def receive_dict(self):
        try:
            payload = self.sock.recv(MAX_MSG_LEN)
            dictionary = loads(payload)
            return (0, dictionary)
        except socket.error as se:
            if se.errno == EINTR: return (1, "Received signal") 
            return (-1, se[1])
        except ValueError:
            return (2, 'Invalid message. Payload size : ' + str(len(payload)))

    # returns (0, None) on success
    # returns (-1, errdesc) on errors
    def connect(self, path):
        try:
            self.sock.connect(path)
            return (0, None)
        except socket.error as se:
            return (-1, "path: " + abspath(path) + " " + se[1] )

    # returns (0, None) on success
    # returns (-1, errdesc) on errors
    def bind(self, path):
        try:
            self.sock.bind(path)
            return (0, None)
        except socket.error as se:
            return (-1, se[1])

    def close(self):
        self.sock.close()
