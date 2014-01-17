from argparse import ArgumentParser
import signal
import errno

from rich_unix_domain_sockets import RichUnixDomainSocket
import common
from common import fatal

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
    
def send_signal_notice(sock):
    request_dict = {}
    request_dict['message_type'] = 'signal_notice'
    request_dict['SIGINT'] = common.sigint
    request_dict['SIGTERM'] = common.sigterm
    return rucs.send_dict(request_dict)

def process_message(message):
    pass

common.setup_signal_recording()
request = parse_arguments()
rucs = RichUnixDomainSocket()

ret, desc = rucs.init()
if ret: fatal(desc) 

ret, desc = rucs.connect(common.DMS_UDS_PATH)
if ret: fatal(desc)

ret, desc = send_request(rucs, request)
if ret: fatal(desc)

done = False
while not done:
    code, retval = rucs.receive_dict()
    if code == 1:
        ret, desc = send_signal_notice(rucs)
        if ret: fatal(desc)
        rucs.close()
        fatal("Received signal, exiting...")
    elif code == 2:
        print "Warning: " + retval
    else:
        process_message(retval)

