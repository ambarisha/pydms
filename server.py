#!/usr/bin/python

from rich_unix_domain_sockets import RichUnixDomainSocket
import common
from os import remove as rm

def cleanup():
    ruds.close()
    rm(common.DMS_UDS_PATH)

def die(message):
    cleanup()
    common.fatal(message)

def process_request(msgdict):
    try:
        url = msgdict['URL']
        target = msgdict['target']
        insist = msgdict['insist']
        print msgdict
        return (0, (url, target, insist))
    except KeyError as ke:
        return (1, ke[0])

def process_signal_notice(msgdict):
    print msgdict
    return (0, "process_signal_notice(): not implemented yet")

# returns (0, None) on success
# returns (1, missing_key) on KeyError
# returns (2, message_type) on unrecognized message type
# returns (-1, desc) on other errors
def handle_message(msgdict):
    try:
        msgtype = msgdict['message_type']
        if msgtype == 'request':
            return process_request(msgdict)
        elif msgtype == 'signal_notice':
            return process_signal_notice(msgdict)
        else:
            return (2, msgtype)
    except KeyError as ke:
        return (1, ke[0])





common.setup_signal_recording()
ruds = RichUnixDomainSocket()
ret, val = ruds.init()
if ret: die("ruds.init() : " + val)

ret, val = ruds.bind(common.DMS_UDS_PATH)
if ret: die("ruds.bind() : " + val)

done = False
while not done:
    ret, val = ruds.receive_dict()
    if ret: die("ruds.receive_dict() : " + val)
    ret, val = handle_message(val)
    if ret: die("handle_message() : " + val)
cleanup()
