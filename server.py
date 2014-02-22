#!/home/bamboo/pydms/bin/python

import urlparse

from rich_unix_domain_sockets import RichUnixDomainSocket
import requests
from os import remove as rm
from thread import start_new_thread
import Queue
from time import sleep

from job_manager import JobManager
from threading import Thread
import common
from common import log
from message import *

def cleanup():
    message = Message(MessageType.DIE)
    job_manager.queue.put(message)
    jm_thread.join()

    while not postbox.empty():
        post_message(postbox.get(), ruds)

    ruds.close()
    rm(common.DMS_UDS_PATH)
    log("Server: Cleaned up")

def die(message):
    cleanup()
    common.fatal(message)

#returns (0, None) on success
#returns (1, key) on KeyError
#returns (2, filename) on IOError
def process_request(addr, msgdict, ruds):
    try:
        url = msgdict['URL']
        target = msgdict['target']
        insist = msgdict['insist']
        updates = msgdict['updates']

        x = urlparse.urlparse(url)
        if x.scheme == '':
            url = "http://" + url

        r = requests.get(url, stream=True)
        f = open(target, 'w')
        for chunk in r.iter_content(8192):
            f.write(chunk)
        f.close()
        return ruds.send_dict({'message_type' : 'response', 'response' : True}, addr)
    except KeyError as ke:
        return (1, ke[0])
    except IOError as ioe:
        return (2, str(ioe) + target)

# returns (0, None) on success
# returns (-1, errdesc) on failure
# returns (-2, missing_field) on invalid request
# returns (-3, IO error message) on IO error opening target file
def dispatch_request(addr, msgdict):
    try:
        message = Message(MessageType.REQUEST)
        message.addr = addr
        message.url = msgdict['URL']
        message.target = msgdict['target']
        message.flags = { 'insist':msgdict['insist'], 'updates':msgdict['updates'] }
        log("Server: New request: " + str(msgdict))

        x = urlparse.urlparse(message.url)
        if x.scheme == '':
            message.url = "http://" + message.url
        job_manager.queue.put(message)
        return ruds.send_dict({'message_type' : 'request_ack'}, addr)    
    except KeyError as ke:
        return (-2, ke[0])
    except IOError as ioe:
        return (-3, str(ioe) + target)

def process_signal_notice(msgdict):
    log("process_signal_notice() not implemented yet")
    return (0, "process_signal_notice(): not implemented yet")

# returns what dispatch_request returns
# returns (-4, message_type) on unknown message type
def handle_message(addr, msgdict, ruds):
    try:
        msgtype = msgdict['message_type']
        if msgtype == 'request':
            #return process_request(addr, msgdict, ruds)
            return dispatch_request(addr, msgdict)
        elif msgtype == 'signal_notice':
            return process_signal_notice(msgdict)
        else:
            return (-4, msgtype)
    except KeyError as ke:
        return (-2, ke[0])

# returns what post_message returns
# dies on error
def post_message(message, ruds):
    ret, val = ruds.send_dict(message.msgdict, message.addr)
    if ret:
        die("Couldn't send message. " + val)
    log("Postman: To " + message.addr + "; " + str(message.msgdict))
    return ret, val

common.setup_signal_recording()
postbox = Queue.Queue()

job_manager = JobManager(postbox)

jm_thread = Thread(target = job_manager.run)
jm_thread.start()

ruds = RichUnixDomainSocket()
ret, val = ruds.init()
if ret:
    die("ruds.init() : " + val)

ret, val = ruds.bind(common.DMS_UDS_PATH)
if ret:
    die("ruds.bind() : " + val)

waiting = False
tries = 1000
while not common.sigint:
    error, val = ruds.wait(0.001)
    if error:
        die(val)

    while not postbox.empty():
        msg = postbox.get()
        if msg.type == MessageType.SEND_MAIL or msg.type == MessageType.UPDATE:
            post_message(msg, ruds)
        elif msg.type == MessageType.FINISHED:
            waiting = True
            log("Server: Job manager Idle")

    if not val:
        if waiting: 
            tries -= 1
            if not tries:
                break
        continue

    tries = 1000
    waiting = False

    ret, val = ruds.recv_dict()
    if ret:
        die("ruds.recv_dict() : " + val)

    ret, val = handle_message(val[0], val[1], ruds)
    if ret: 
        die("handle_message() : " + val)
cleanup()
