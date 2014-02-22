from os import remove as rm
from os.path import exists
from time import sleep
from argparse import ArgumentParser
from random import sample
from string import lowercase
from subprocess import Popen as start
import signal

from rich_unix_domain_sockets import RichUnixDomainSocket
import common
from common import fatal

def cleanup():
    ruds.close()
    rm(addr)

def die(message):
    cleanup()
    fatal(message)

def parse_arguments():
    parser = ArgumentParser()
    parser.add_argument("url", help="URL to be downloaded")
    parser.add_argument("target", help="Local filename to be saved as")
    parser.add_argument("-i", "--insist", help="Insist on using the mirror in the URL", action="store_true")
    parser.add_argument("-u", "--updates", help="Show status updates. Not recommended incase of use in scripts", action="store_true")
    args = parser.parse_args()
    return args

# returns (O, None) on success
# returns (1, None) on signal interruption
# returns (2, errdesc) on invalid ack messages
# returns (-1, errdesc) on other errors
def send_request(ruds, request):
    request_dict = {} 
    request_dict['message_type'] = 'request'
    request_dict['URL'] = request.url
    request_dict['target'] = request.target
    request_dict['insist'] = request.insist
    request_dict['updates'] = request.updates

    err, desc = ruds.send_dict(request_dict)
    if err:
        return err, desc

    err, val = ruds.recv_dict(remote = common.DMS_UDS_PATH, timeout = 10)
    if err:
        return err, val
     
    addr, msgdict = val
    if msgdict['message_type'] != 'request_ack':
        return -3, "Request not acknowledged"
    return (0, None)

# returns what ruds.send_dict does
def send_signal_notice(sock):
    request_dict = {}
    request_dict['message_type'] = 'signal_notice'
    request_dict['SIGINT'] = common.sigint
    request_dict['SIGTERM'] = common.sigterm
    return ruds.send_dict(request_dict)

# returns (0, None) on success response
# returns (1, None) on an update message
# returns (-1, message_type) on unknown message type
# returns (-2, error) on failure response
def process_message(addr, msgdict):
    if msgdict['message_type'] == 'response':
        if msgdict['response'] == True:
            return (0, None)
        elif msgdict['response'] == False:
            return (-2, msgdict['error'])

    elif msgdict['message_type'] == 'update':
        curbytes = msgdict['bytes_received']
        totbytes = msgdict['total_bytes']
        speed = str(int(msgdict['speed']))

        if totbytes:
            percent = curbytes * 100 / totbytes
            print "%4s" % str(percent) + ' '.join(["% complete of", str(msgdict['total_bytes']), "bytes at", speed,"bytes/sec"])
        else:
            print "%10s" % str(curbytes) + ' bytes received at' + speed + ' bytes/sec. Total size unknown.'

        return (1, None)
    return (-1, msgdict['message_type'])

def checkup_on_dms():
    if not exists(common.DMS_UDS_PATH):
        start(['./server.py'])
    for i in xrange(30):
        if exists(common.DMS_UDS_PATH): 
            return
        sleep(0.1)
    fatal("Error: Could not start pydms service. Giving up.")

checkup_on_dms()
common.setup_signal_recording()
request = parse_arguments()
ruds = RichUnixDomainSocket()

ret, desc = ruds.init()
if ret:
    fatal(desc) 

addr = ''.join(sample(lowercase, 10))
ret, desc = ruds.bind(addr)
if ret:
    die(desc)

checkup_on_dms()

ret, desc = ruds.connect(common.DMS_UDS_PATH)
if ret:
    die(desc)

ret, desc = send_request(ruds, request)
if ret:
    die(desc)

done = False
while not done:
    code, val = ruds.recv_dict(remote = common.DMS_UDS_PATH)
    if code == 1:
        ret, desc = send_signal_notice(ruds)
        if ret:
            die(desc)

    elif code == 2:
        log("Warning: " + val)

    else:
        ret, desc = process_message(val[0], val[1])
        if ret == 0:
            done = True
        elif ret == -1:
            log("Client: Unknown message of type '" + desc + "' received")
        elif ret == -2:
            die("Error: " + desc)
cleanup()
