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
        message = Message(MessageType.SEND_MAIL)
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
                    if msg.type == MessageType.SIGNAL:
                        return (1, "SIGNAL message from client")#msg.signal)
                    elif msg.type == MessageType.DIE:
                        return (2, "Death warrant received")
                if send_updates:
                    self._send_update(curbytes, totbytes, speed)
                
            return (0, {'speed':speed, 'filesize':totbytes})

    def _get_credentials(self):
        message = Message(MessageType.SEND_MAIL)
        message.msgdict = {}
        message.msgdict['message_type'] = 'auth_request'
        message.addr = self._client
        self._postman.put(message)
        
        try:
            response = self.queue.get(True, timeout=1)
            if response.type != MessageType.AUTH_RESPONSE:
                log("Worker: Invalid message received. MessageType: " + str(response.type))
                return None
            return response.credentials
        except Queue.Empty:
            return None

    # returns (0, {'speed', 'filesize'}) on success
    # returns (-3, errdesc) if auth creds not received from client
    # returns (-4, errdesc) if auth failed after 3 retries
    # returns (-2, errdesc) on IOError wrt `target`
    # returns (-1, errdesc) if request returned other than 200 status
    # returns (1, signal number) on signal message from client
    # returns (2, errdesc) on DIE message interruption
    def _download(self, url, target, send_updates = False):
        try:
            # Todo: Assert the domain is self._site
            response = self._session.head(url, allow_redirects = True)
            tries = 3
            # Todo: Recheck if 401 status is returned when an auth fails
            while response.status_code == 401 and tries:
                credentials = self._get_credentials()
                if not credentials:
                    return (-3, "Couldn't get authentication credentials for client")
                response = self._session.head(url, allow_redirects=True, auth=HTTPBasicAuth(credentials))
                if response.status_code == 401:
                    tries -= 1
                    continue
            if not tries:
                return (-4, "Authentication failed")

            totbytes = int(response.headers['content-length']) if 'content-length' in response.headers else 0

            # Todo: What if server doesn't allow cookies and authentication is needed?
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

    def _finish(self, status, remote, speed=None, filesize=None):
        report = Message(MessageType.JOB_REPORT)
        report.status = status 
        report.sender = self
        report.site = self._site
        if status == 0:
            report.filesize = filesize
            report.speed = speed
        self._employer.put(report)

        response_mail = Message(MessageType.SEND_MAIL)
        response_mail.msgdict = {'message_type' : 'response',
                                 'response' : True if status == 0 else False}
        if status == -1:
            response_mail.msgdict['error'] = "HTTP request failed"
        elif status == -2:
            response_mail.msgdict['error'] = "Couldn't not open local target file for writing"
        elif status == -3:
            response_mail.msgdict['error'] = "Authentication credentials not received"
        elif status == -4:
            response_mail.msgdict['error'] = "Authentication failed"
        elif status == 1:
            response_mail.msgdict['error'] = "Interrupted by signal"
        elif status == 2:
            response_mail.msgdict['error'] = "DMS exited unexpectedly"

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

                    if ret == 0:
                        self._finish(ret, msg.client, speed=val['speed'], filesize=val['filesize'])
                    else:
                        self._finish(ret, msg.client)
                elif msg.type == MessageType.SIGNAL:
                    log("Worker: Signal notice received out of context from " + msg.client)
                elif msg.type == MessageType.DIE:
                    return True
                elif msg.type == MessageType.RELEIVE:
                    return False
                else:
                    log("Worker: Invalid message received")
            except Queue.Empty as e:
                break
            except Exception as e:
                print "Worker:", e
    
    def run(self):
        done = self._process_queue()
        if done:
            return 

        resignation = Message(MessageType.RESIGNATION)
        resignation.sender = self
        self._employer.put(resignation)

        self._process_queue(notice_period=True)
