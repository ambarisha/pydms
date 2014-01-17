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

def handle_message(cruds):
    print val

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
    handle_message(val)
cleanup()
