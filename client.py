from sys import exit
from argparse import ArgumentParser
import signal

from rich_unix_domain_sockets import RichUnixDomainSocket

UDS_PATH = "dms.uds"

def fatal(msg):
    print msg
    exit()

def parse_arguments():
    parser = ArgumentParser()
    parser.add_argument("url", help="URL to be downloaded")
    parser.add_argument("target", help="Local filename to be saved as")
    parser.add_argument("-i", "--insist", help="Insist on using the mirror in the URL", action="store_true")
    args = parser.parse_args()
    return args

def send_request(rucs, request):
    request_dict = {} 
    request_dict['message_type'] = 'request'
    request_dict['URL'] = request.url
    request_dict['target'] = request.target
    request_dict['insist'] = request.insist
    return rucs.send_dict(request_dict)
    
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

def send_signal_notice(sock):
    request_dict = {}
    request_dict['message_type'] = 'signal_notice'
    request_dict['SIGINT'] = sigint
    request_dict['SIGTERM'] = sigterm
    return rucs.send_dict(request_dict)

def process_message(message):
    pass

setup_signal_recording()
request = parse_arguments()
rucs = RichUnixDomainSocket()

ret, desc = rucs.init()
fatal(desc) if ret

ret, desc = rucs.connect(UDS_PATH)
fatal(desc) if ret

ret, desc = send_request(rucs, request)
fatal(desc) if ret

done = False
while not done:
    code, retval = rucs.recieve_dict()
    if code == 1:
        ret, desc = send_signal_notice(rucs)
        fatal(desc) if ret
    elif code == 2:
        print "Warning: " + retval
    else:
        process_message(retval)

