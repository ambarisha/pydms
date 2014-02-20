
import Queue
from thread import start_new_thread
from urlparse import urlparse, ParseResult
from datetime import datetime, timedelta
from json import load, dump
from threading import Thread

from common import log, fatal
from worker import Worker
from message import Message, MessageType

def datetime_encode(obj):
    if isinstance(obj, datetime):
        dtbase = datetime(1970, 1, 1)
        return int((obj - dtbase).total_seconds())
    raise TypeError

def profile_decode(dictionary):
    d = {}
    for key in dictionary:
        d[key] = []
        for reading in dictionary[key]:
            d[key].append((datetime.utcfromtimestamp(reading[0]), reading[1]))
    return d

class Job:
    def __init__(self, addr, url, target, flags = {'insist':False, 'updates':False}):
        self.url = url
        self.original_url = url
        self.target = target
        self.flags = flags
        self.addr = addr
        self.status = False
        self.site = urlparse(url).netloc

    def update_url(self, site):
        x = urlparse(self.url)
        y = ParseResult(scheme = x.scheme, netloc = site, path = x.path, params = x.params, query = x.query, fragment = x.fragment)
        self.site = site
        self.url = y.geturl()
        
class JobManager:
    def __init__(self, postman, recent = timedelta(days = 7), filename = '.dms_profiles'):
        self.queue = Queue.Queue()
        self._postman = postman
        self._jobs = []
        self._workers = {}
        self._worker_status = {}
        self._threads = {}
        self._assignments = {}
        self._preferences = []
        self._recent = recent
        self._filename = filename
        ret, val = self._load(self._filename)
        if ret: fatal("Could not load profiles file: " + self._filename)

    def _load(self, filename):
        try:
            f = open(filename, 'r')
            self._record = load(f, object_hook = profile_decode)
            self._summarize()        
            return (0, None)
        except IOError as ioe:
            return (1, ioe.strerror + ": " + filename)
        except ValueError as ve:
            return (2, filename)

    def _save(self, filename):
        try:
            f = open(filename, 'w')
            dump(self._record, f, default = datetime_encode)
        except IOError as ioe:
            return (1, ioe.strerr + ": " + filename)

    def _summarize(self):
        self._preferences = []
        for site in self._record:
            readings = [x[1] for x in self._record[site] if x[0] - datetime.now() < self._recent]
            self._preferences.append((site, sum(readings, 0.0) / len(readings)))
        def compare(a, b):
            if a[1] == b[1]:
                return 0
            elif a[1] > b[1]:
                return -1
            elif a[1] < b[1]:
                return 1

        self._preferences.sort(cmp = compare)

    def _update(self, site, speed, filesize):
        if not site in self._record:
            return
        self._record[site].append((datetime.now(), speed))     
        self._summarize()

    def _pick_site(self):
        for (site, profile) in self._preferences:
            if site not in self._preferences or self._worker_status[self._workers[site]] == False:
                return site
        return self._preferences[0][0]

    def _hire(self, site):
        worker = Worker(site, self.queue, self._postman) 
        self._threads[worker] = Thread(target = worker.run)
        self._threads[worker].start()
        self._workers[site] = worker
        self._worker_status[worker] = False
        return worker

    def _assign(self, worker, job=None):
        # Todo: Assert worker status == False
        if not job:
            for j in self._jobs:
                if j.status == False:
                    if (j.flags['insist'] == True and j.site == worker.site) or (j.flags['insist'] == False): 
                        job = j
                        break
            if job == None:
                return
        message = Message(MessageType.NEW_JOB)
        message.url = job.url
        message.target = job.target
        message.client = job.addr
        message.send_updates = job.flags['updates']
        worker.queue.put(message)

        self._assignments[worker] = job
        self._assignments[job] = worker
        self._worker_status[worker] = True
        job.status = True
        return

    def _signoff(self, worker, status):
        job = self._assignments[worker]
        self._assignments.pop(worker)
        self._assignments.pop(job)
        if worker in self._worker_status:
            self._worker_status[worker] = False
        self._jobs.remove(job)
        if len(self._jobs) == 0:
            self._postman.put(Message(MessageType.FINISHED))
        log("Job report: " + job.url + " Worker: " + str(id(worker)) + " Status: " + str(status))
   
    def _dispatch(self, job):
        self._jobs.append(job)
        if not job.flags['insist']:
            site = self._pick_site()
            job.update_url(site)
        if job.site in self._workers:
            if self._worker_status[self._workers[job.site]] == False:
                self._assign(self._workers[job.site], job)
        else:
            worker = self._hire(job.site)
            self._assign(worker, job)

    def _accept_resignation(self, worker):
        self._worker_status.pop(worker)
        self._threads.pop(worker)
        self._workers.pop(worker.site)
        worker.queue.put(Message(MessageType.RELEIVE))

    def run(self):
        done = False
        while not done:
            msg = self.queue.get()
            if msg.type == MessageType.REQUEST:
                self._dispatch(Job(msg.addr, msg.url, msg.target, msg.flags)) 
            elif msg.type == MessageType.JOB_REPORT:
                self._update(msg.site, msg.speed, msg.filesize)
                self._signoff(msg.sender, msg.status)
                self._assign(msg.sender)
            elif msg.type == MessageType.SIGNAL:
                self._workers[msg.addr].queue.put(msg)
            elif msg.type == MessageType.DIE:
                for worker in self._workers:
                    self._workers[worker].queue.put(Message(msg.type))
                done = True
                log("JobManager: Recieved DIE message")
            elif msg.type == MessageType.RESIGNATION:
                self._accept_resignation(msg.sender)
            else:
                log("JobManager: Invalid message received [Type: " + str(msg.type) + "]")
        for worker in self._threads:
            self._threads[worker].join()
        self._save(self._filename)
