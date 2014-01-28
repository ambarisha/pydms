
import Queue
from thread import start_new_thread
from urlparse import urlparse, ParseResult

from worker import Worker
from message import Message, MessageType

class Job:
    def __init__(self, addr, url, target, insist = False):
        self.url = url
        self.original_url = url
        self.target = target
        self.insist = insist
        self.addr = addr
        self.status = False
        self.site = urlparse(url).netloc

    def update_url(self, site):
        x = urlparse(self.url)
        y = ParseResult(scheme = x.scheme, netloc = site, path = x.path, params = x.params, query = x.query, fragment = x.fragment)
        self.site = site
        self.url = y.geturl()
        
class JobManager:
    def __init__(self, postman, profile_manager):
        self.queue = Queue.Queue()
        self._postman = postman
        self._profile_manager = profile_manager
        self._jobs = []
        self._workers = {}
        self._worker_status = {}
        self._assignments = {}
        self._profiles = []

    def _signoff(self, worker, status):
        # Todo: Probably its better to log the status
        job = self._assignments[worker]
        self._assignments.remove(worker)
        self._assignments.remove(job)
        self._worker_status[worker] = False
        self._jobs.remove(job)

    def _assign(self, worker, job = None):
        # Todo: Assert worker status == False
        if not job:
            for j in self._jobs:
                if j.status == False:
                    if (j.insist == True and j.site == worker.site) or (j.insist == False): 
                        job = j
                        break
            if job == None:
                return

        message = Message(MessageType.NEW_JOB)
        message.url = job.url
        message.target = job.target
        worker.queue.put(message)

        self._assignments[worker] = job
        self._assignments[job] = worker
        self._worker_status[worker] = True
        job.status = True
        return
    
    def _hire(self, site):
        worker = Worker(self.queue, self.queue, self._postman, self._profile_manager) 
        start_new_thread(worker.run, ())
        self._workers[site] = worker
        self._worker_status[worker] = False
        return worker

    def _pick_site(self):
        for (site, profile) in self._profiles:
            if site not in self._workers or self._worker_status[self._workers[site]] == False:
                return site
        return self._profiles[0][0]

    def _dispatch(self, job):
        if not job.insist:
            site = self._pick_site()
            job.update_url(site)
        if job.site in self._workers:
            if self._workers[job.site] == False:
                self._assign(self._workers[job.site], job)
        else:
            worker = self._hire(site)
            self._assign(worker, job)

    def _update_profiles(self, summary):
        def compare(a, b):
            if a[1] == b[1]:
                return 0
            elif a[1] > b[1]:
                return -1
            elif a[1] < b[1]:
                return 1
        summary.sort(cmp = compare)
        self._profiles = summary
        # Todo: What if the new summary excludes some of the old entries??

    def run(self):
        done = False
        while not done:
            msg = self.queue.get()
            if msg.type == MessageType.REQUEST:
                self._dispatch(Job(msg.addr, msg.url, msg.target, msg.insist)) 
            elif msg.type == MessageType.PROFILE_SUMMARY:
                self._update_profiles(msg.summary)
            elif msg.type == MessageType.JOB_REPORT:
                self._signoff(msg.sender, msg.status)
                self._assign(msg.sender)
            elif msg.type == MessageType.SIGNAL:
                self._workers[msg.addr].queue.put(msg)
            elif msg.type == MessageType.DIE:
                for worker in self._workers:
                    worker.queue.put(Message(msg))
                done = True
            else:
                log("JobManager: Invalid message received [Type: " + str(msg.type) + "]")
