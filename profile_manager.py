import Queue
from datetime import datetime, timedelta
from json import load, dump

from message import Message, MessageType
from common import log, fatal

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

class ProfileManager:
    def __init__(self, recent = timedelta(days = 7), filename = '.dms_profiles'):
        self.queue = Queue.Queue()
        self._recent = recent
        self._filename = filename
        ret, val = self._load(self._filename)
        if ret: fatal("Could not load profiles file: " + self._filename)

    def _load(self, filename):
        try:
            f = open(filename, 'r')
            self._record = load(f, object_hook = profile_decode)
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

    def _update(self, site, speed, filesize):
        self._record[site].append((datetime.now(), speed))     

    def _summarize(self):
        summary = []
        for site in self._record:
            readings = [x[1] for x in self._record[site] if x[0] - datetime.now() < self._recent]
            summary.append((site, sum(readings, 0.0) / len(readings)))
        return summary

    def run(self, job_manager):
        summary = self._summarize()
        summary_response = Message(MessageType.PROFILE_SUMMARY)
        summary_response.summary = summary
        job_manager.put(summary_response)
    
        while True:
            msg = self.queue.get()
            if msg.type == MessageType.PROFILE_UPDATE:
                _update(msg.site, msg.speed, msg.filesize)
                summary = self._summarize()
                summary_response = Message(MessageType.PROFILE_SUMMARY)
                summary_response.summary = summary
                job_manager.put(summary_response)
            elif msg.type == MessageType.DIE:
                break
            else:
                log("Invalid message received")
        _save(self._filename)
