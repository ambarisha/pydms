import socket
from errno import EINTR
from json import loads, dumps
from os.path import abspath
from select import select, error as select_error


MAX_MSG_LEN = 2048

class RichUnixDomainSocket:
    # returns (0, None) on success
    # returns (-1, errdesc) on failure
    def init(self, sock = None):
        if sock:
            self._sock = sock
        else:
            try:
                self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            except socket.error as se:
                return (-1, se[1])
        return (0, None)
 
    # returns (0, None) on success
    # returns (-1, errdesc) on failure
    def send_dict(self, dictionary, addr = None):
        payload = dumps(dictionary)
        try:
            if addr:
                self._sock.sendto(payload, addr)
            else:
                self._sock.send(payload)
        except socket.error as se:
            return -1, se[1] 
        return 0, None

    # returns (0, dictionary) on success
    # returns (1, None) on signal interruption
    # returns (2, errdesc) on invalid message
    # returns (-1, errdesc) on other errors
    def recv_dict(self, remote = None, timeout = None):
        try:
            if timeout:
                self.wait(timeout)

            done = False
            while not done:
                payload, addr = self._sock.recvfrom(MAX_MSG_LEN)
                if remote and addr != remote:
                    continue
                done = True

            dictionary = loads(payload)
            return (0, (addr, dictionary))
        except socket.error as se:
            if se.errno == EINTR: return (1, "Received signal") 
            return (-1, se[1])
        except ValueError:
            return (2, 'Invalid message. Payload size : ' + str(len(payload)))

    # returns (0, None) on success
    # returns (-1, errdesc) on errors
    def connect(self, path):
        try:
            self._sock.connect(path)
            return (0, None)
        except socket.error as se:
            return (-1, "path: " + abspath(path) + " " + se[1] )

    # returns (0, None) on success
    # returns (-1, errdesc) on errors
    def bind(self, path):
        try:
            self._sock.bind(path)
            return (0, None)
        except socket.error as se:
            return (-1, se[1])

    def close(self):
        self._sock.close()

    # returns (0, have_mail) on success
    # returns (-1, errdesc) on errors
    def wait(self, tosec):
        try:
            rready, wready, xready = select([self._sock], [], [], tosec)
            if self._sock in rready:
                return (0, True)
            return (0, False)
        except select_error as se:
            return (-1, "Signal recieved. Exiting.")
