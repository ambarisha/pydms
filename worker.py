import requests
import Queue
from os import remove as rm
from common import fatal

from message import Message, MessageType

class Worker:
    max_wait_timeout = 5

    def __init__(self, site, employer, postman):
        self.queue = Queue.Queue()
        self._site = site
        self._employer = employer
        self._postman = postman
        self._session = requests.Session()

    def _download(self, url, target):
        try:
            # Todo: Assert the domain is self._site
            r = self._session.get(url, stream = True, allow_redirects = True)
            if r.status_code != 200: 
                return (-1, r.status_code)

            f = open(target, 'w')
            for chunk in r.iter_content(16384):
                f.write(chunk)
                if not self.queue.empty():
                    msg = self.queue.get()
                    # Todo: Assert its a signal notice or a death warrant
                    f.close()
                    rm(target)
                    if msg.type == MessageType.DIE:
                        return (3, "Death warrant received")
                    return (2, msg.signal)
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
        self._postman.put(response_mail)

    def run(self):
        while True:
            try:
                msg = self.queue.get(True, self.max_wait_timeout)
            except Queue.Empty as e:
                break

            try:
                if msg.type == MessageType.NEW_JOB:
                    ret, val = self._download(msg.url, msg.target)
                    if ret < 0: fatal(val)
                    self._finish(ret, 3000000, 1000000, msg.client) # Todo: Temporary
                    if ret == 2: return
                elif msg.type == MessageType.SIGNAL:
                    log("Signal notice received out of context from " + msg.client)
                elif msg.type == MessageType.DIE:
                    log("Received request to die. Dying.")
                    break
                else:
                    log("Invalid message received")
            except Exception as e:
                print e
        
        resignation = Message(MessageType.RESIGNATION)
        resignation.sender = self
        self._employer.put(resignation)
