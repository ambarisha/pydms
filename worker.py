import requests
from datetime import datetime
import Queue
from os import remove as rm
from common import fatal, log

from message import Message, MessageType

class Worker(object):
    max_wait_timeout = 5
    
    @property
    def site(self):
        return self._site

    def __init__(self, site, employer, postman):
        self.queue = Queue.Queue()
        self._site = site
        self._employer = employer
        self._postman = postman
        self._session = requests.Session()

    def _send_update(self, curbytes, totbytes, speed):
        message = Message(MessageType.UPDATE)
        message.msgdict = {}
        message.msgdict['message_type'] = 'update'
        message.msgdict['bytes_received'] = curbytes
        message.msgdict['total_bytes'] = totbytes 
        message.msgdict['speed'] = speed
        message.addr = self._client
        self._postman.put(message)

    def _get_content(self, r, f, send_updates, totbytes):
            curbytes = 0
            stdt = datetime.now()

            for chunk in r.iter_content(16384):
                enddt = datetime.now()
                delta = (enddt - stdt).total_seconds()
                curbytes += len(chunk)
                speed = curbytes / delta

                f.write(chunk)

                if not self.queue.empty():
                    msg = self.queue.get()
                    # Todo: Assert its a signal notice or a death warrant
                    if msg.type == MessageType.DIE:
                        return (3, "Death warrant received")
                    return (2, msg.signal)
                
                if send_updates:
                    self._send_update(curbytes, totbytes, speed)
                
            return (0, {'speed':speed, 'filesize':totbytes})

    def _download(self, url, target, send_updates = False):
        try:
            # Todo: Assert the domain is self._site
            headers = self._session.head(url, allow_redirects = True).headers
            totbytes = int(headers['content-length']) if 'content-length' in headers else 0

            r = self._session.get(url, stream = True, allow_redirects = True)
            if r.status_code != 200: 
                return (-1, r.status_code)

            f = open(target, 'w')

            ret, val = self._get_content(r, f, send_updates, totbytes)
            f.close()
            if not ret:
                rm(target)

            return(ret, val)
        except IOError as ioe:
            return (-2, target + ": " + str(ioe))

    def _finish(self, status, speed, filesize, remote):
        report = Message(MessageType.JOB_REPORT)
        report.status = status 
        report.sender = self
        report.filesize = filesize
        report.speed = speed
        report.site = self._site
        self._employer.put(report)

        response_mail = Message(MessageType.SEND_MAIL)
        response_mail.msgdict = {'message_type' : 'response',
                                 'response' : True if status == 0 else False}
        response_mail.addr = remote
        self._postman.put(response_mail)

    def _process_queue(self, notice_period=False):
        while True:
            try:
                if notice_period:
                    msg = self.queue.get(block=True)
                else:
                    msg = self.queue.get(block=True, timeout=self.max_wait_timeout)

                if msg.type == MessageType.NEW_JOB:
                    self._client = msg.client
                    ret, val = self._download(msg.url, msg.target, msg.send_updates)
                    if ret < 0: fatal(val)
                    self._finish(ret, val['speed'], val['filesize'], msg.client)
                    if ret == 2 or ret == 3: return False
                elif msg.type == MessageType.SIGNAL:
                    log("Signal notice received out of context from " + msg.client)
                elif msg.type == MessageType.DIE:
                    return True
                elif msg.type == MessageType.RELEIVE:
                    return False
                else:
                    log("Invalid message received")
            except Queue.Empty as e:
                break
            except Exception as e:
                print e
    
    def run(self):
        done = self._process_queue()
        if done: return 

        resignation = Message(MessageType.RESIGNATION)
        resignation.sender = self
        self._employer.put(resignation)

        self._process_queue(notice_period=True)
