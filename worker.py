import requests
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

    def _send_update(self, curbytes, totbytes):
        message = Message(MessageType.UPDATE)
        message.msgdict = {}
        message.msgdict['message_type'] = 'update'
        message.msgdict['percent'] = curbytes * 100 / totbytes
        message.msgdict['total_bytes'] = totbytes 
        message.addr = self._client
        self._postman.put(message)

    def _download(self, url, target, send_updates = False):
        try:
            # Todo: Assert the domain is self._site
            # Todo: What if 'content-length' is not available in headers?
            headers = self._session.head(url, allow_redirects = True).headers
            totbytes = int(headers['content-length'])
            r = self._session.get(url, stream = True, allow_redirects = True)
            if r.status_code != 200: 
                return (-1, r.status_code)

            f = open(target, 'w')
            curbytes = 0
            for chunk in r.iter_content(16384):
                f.write(chunk)
                curbytes += len(chunk)

                if not self.queue.empty():
                    msg = self.queue.get()
                    # Todo: Assert its a signal notice or a death warrant
                    f.close()
                    rm(target)
                    if msg.type == MessageType.DIE:
                        return (3, "Death warrant received")
                    return (2, msg.signal)
                
                if send_updates:
                    self._send_update(curbytes, totbytes)

            f.close()
            return (0, None)

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
        #print "Response Mail: ", response_mail.msgdict, response_mail.addr
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
                    self._finish(ret, 3000000, 1000000, msg.client) # Todo: Temporary
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
