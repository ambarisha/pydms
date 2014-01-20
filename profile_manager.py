from datetime import datetime, timedelta
from common import log
from json import load, dump

class ProfileManager:
    def __init__(self, recent = timedelta(days = 7), filename = '.dms_profiles'):
        self.queue = Queue.queue()
        self._recent = recent
        self._filename = filename
        _load(self._filename)
    
    def _load(self, filename):
        try:
            f = open(filename, 'r')
            self._record = load(f)
        except IOError as ioe:
            return (1, ioe.strerr + ": " + filename)
        except ValueError as ve:
            return (2, filename)

    def _save(self, filename):
        try:
            f = open(filename, 'w')
            dump(self._record, f)
        except IOError as ioe:
            return (1, ioe.strerr + ": " + filename)
    def _update(self, site, speed, filesize):
        self._record[site].append((datetime.now(), speed))     

    def _summarize(self):
        summary = {}
        for site in self._record:
            summary[site] = [x[1] for x in self._record[site] if x[0] - datetime.now() < self._recent]
            summary[site] = sum(summary[site], 0.0) / len(summary[site])
        return summary

    def run(self):
        done = False
        while not done:
            msg = self.queue.get()
            if msg.type == MessageType.PROFILE_SUMMARY_REQUEST:
                summary = _summarize()
                summary_response = Message(MessageType.PROFILE_SUMMARY)
                summary_response.summary = summary
                self._job_manager.post(summary_response)
            elif msg.type == MessageType.PROFILE_UPDATE:
                _update(msg.site, msg.speed, msg.filesize)
            elif msg.type == MessageType.DIE:
                break
            else:
                log("Invalid message received")
        _save(self._filename)
