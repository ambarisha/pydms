from os import remove as rm
from argparse import ArgumentParser
from random import sample
from string import lowercase
import signal

from rich_unix_domain_sockets import RichUnixDomainSocket
import common
from common import fatal

def cleanup():
    rucs.close()
    rm(addr)

def die(message):
    cleanup()
    fatal(message)

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
    err, desc = rucs.send_dict(request_dict)
    if err: return err, desc

    err, val = rucs.recv_dict(remote = common.DMS_UDS_PATH, timeout = 10)
    if err: return err, val
   
    addr, msgdict = val
    if msgdict['message_type'] != 'request_ack':
        return -3, "Request not acknowledged"
    return (0, None)

def send_signal_notice(sock):
    request_dict = {}
    request_dict['message_type'] = 'signal_notice'
    request_dict['SIGINT'] = common.sigint
    request_dict['SIGTERM'] = common.sigterm
    return rucs.send_dict(request_dict)

def process_message(addr, msgdict):
    if msgdict['message_type'] == 'response':
        if msgdict['response'] == True:
            return (0, "Finished")
        elif msgdict['response'] == False:
            return (-2, "Request could not be processed")
    print "from:", addr
    print "message:", msgdict
    return (-1, "Failure")

common.setup_signal_recording()
request = parse_arguments()
rucs = RichUnixDomainSocket()

ret, desc = rucs.init()
if ret: fatal(desc) 

addr = ''.join(sample(lowercase, 10))
ret, desc = rucs.bind(addr)
if ret: die(desc)

ret, desc = rucs.connect(common.DMS_UDS_PATH)
if ret: die(desc)

ret, desc = send_request(rucs, request)
if ret: die(desc)

done = False
while not done:
    code, val = rucs.recv_dict(remote = common.DMS_UDS_PATH)
    if code == 1:
        ret, desc = send_signal_notice(rucs)
        if ret: die(desc)
        die("Received signal, exiting...")
    elif code == 2:
        print "Warning: " + val
    else:
        ret, desc = process_message(val[0], val[1])
        if not ret:
            done = True
        elif ret == -2:
            print "Error: " + desc
            done = True
cleanup()
