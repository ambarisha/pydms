
class MessageType:
    NEW_JOB = 1
    JOB_REPORT = 2
    SIGNAL = 3
    SEND_MAIL = 4
    PROFILE_SUMMARY = 5
    UPDATE_PROFILE = 6
    RESIGNATION = 7
    REQUEST = 8
    DIE = 9

class Message:
    def __init__(self, msgtype):
        self.type = msgtype
