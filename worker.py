
import requests
import Queue
from os import remove as rm

class Worker:
    max_wait_timeout = 5

    def __init__(self, id ):
        self.queue = Queue.queue()

    def _download(self, url, target):
        try:
            r = self._session.get(url, stream = True, allow_redirects = True)
            if r.status_code != 200: 
                return (1, r.status_code)

            f = open(target, 'w')
            for chunk in r.iter_content(16384):
                f.write(chunk)
                if not self.queue.empty():
                    msg = self.queue.get()
                    # Todo: Assert its a signal notice
                    f.close()
                    rm(target)
                    return (3, msg.signal)
            f.close()
            return (0, None)
        except IOError as ioe:
            return (2, target + ": " + ioe.strerror)

    def _init(self, site):
        self._session = requests.Session()
        self._site = site

    def _finish(self, status, speed, filesize):
        update = Message(MessageType.UPDATE_PROFILE)
        update.file_size = filesize
        update.speed = speed 
        update.site = self._site
        profile_manager.queue.post(update)

        report = Message(MessageType.JOB_REPORT)
        report.status = status 
        report.sender = id(self)
        employer.queue.post(report)

        response_mail = Message(MessageType.SEND_MAIL)
        response_mail.msgdict = {'message_type' : 'response',
                                 'response' : True if status == 1 else False}
        response_mail.addr = msg.remote_addr
        postman.queue.post(response_mail)
        return


    def run(self):
        done = False
        while True:
            try:
                msg = self.queue.get(True, max_wait_timeout)
            except Empty as e:
                break

            if msg.type == MessageType.NEW_JOB:
                ret, val = _download(msg.url, msg.target)
                if ret != 0 and ret != 3: die(val)
                _finish(ret, 3000000, 1000000) # Todo: Temporary
            elif msg.type == MessageType.SIGNAL:
                pass
                # Todo: Log that an out of place signal message was received
            else:
                # Todo: Log that an invalid message was received
                pass

        resignation = Message(MessageType.RESIGNATION)
        resignation.sender = id(self)
        employer.queue.post(resignation)

