import socket
from json import loads, dumps
from os.path import abspath

class RichUnixDomainSocket:
    # returns (0, None) on success
    # returns (-1, errdesc) on failure
    def init(self, sock = None, listen_queue = 5)
        self.listen_queue = listen_queue
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
            return (1, None) if se.errno == errno.EINTR
            return (-1, se[1])
        except ValueError:
            return (2, 'Malformed message received. Payload size : ', str(len(payload)))

    # returns (0, None) on success
    # returns (-1, errdesc) on errors
    def connect(self, path):
        try:
            self.sock.connect(path)
            return (0, None)
        except socket.error as se:
            return (-1, se[0] + "path : " + abspath(path))

    # returns (0, None) on success
    # returns (-1, errdesc) on errors
    def setup(self, path):
        try:
            self.sock.bind(path)
            self.sock.listen(self.listen_queue)
        except socket.error as se:
            return (-1, se[1])

    def close(self):
        self.sock.close()
