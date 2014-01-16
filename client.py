from os.path import abspath
from sys import exit
from json import loads, dumps
import socket
from argparse import ArgumentParser
import signal

UDS_PATH = "dms.uds"

class RichUnixClientSocket:
    def __init__(self, sock = None):
        if sock:
            self.sock = sock
        else:
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)

    def send_dict(self, dictionary):
        payload = json.dumps(dictionary)
        self.sock.send(payload)

    def receive_dict(self):
        try:
            payload = self.sock.recv(MAX_MSG_LEN)
            dictionary = json.loads(payload)
        except socket.error as se:
            if se.errno == errno.EINTR:
                return False, errno.EINTR
            print "Error: Could not receive message from server: " + se[0] +" : ErrorNo: " + str(se[1]) 
            exit()   
        except ValueError:
            print 'Error: Malformed message received. Payload size : ', str(len(payload))
            exit()
        return True, dictionary

    def connect(self, path):
        try:
            self.sock.connect(path)
        except socket.error as se:
            print "Error: Could not connect to socket at " + abspath(path)
            exit()
       
    def close(self):
        self.sock.close()

def parse_arguments():
    parser = ArgumentParser()
    parser.add_argument("url", help="URL to be downloaded")
    parser.add_argument("target", help="Local filename to be saved as")
    parser.add_argument("-i", "--insist", help="Insist on using the mirror in the URL", action="store_true")
    args = parser.parse_args()
    return args

def send_request(rucs, request):
    request_dict = {} 
    request_dict['URL'] = request.url
    request_dict['target'] = request.target
    request_dict['insist'] = request.insist
    rucs.send_dict(request_dict)
    return 

def set_signal_flag(sig):
    if sig == signal.SIGINT:
        sigint = True
    elif sig == signal.SIGSEGV:
        sigsegv = True
    elif sig == signal.SIGTERM:
        sigterm = True

def setup_signal_recording():
    signal.signal(signal.SIGABRT, signal.SIG_IGN)
    signal.signal(signal.SIGFPE, signal.SIG_IGN)
    signal.signal(signal.SIGINT, set_signal_flag)
    signal.signal(signal.SIGILL, signal.SIG_IGN)
    signal.signal(signal.SIGSEGV, set_signal_flag)
    signal.signal(signal.SIGTERM, set_signal_flag)
    return


setup_signal_recording()
request = parse_arguments()
rucs = RichUnixClientSocket()
rucs.connect(UDS_PATH)
send_request(rucs, request)

done = False

