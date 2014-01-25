
class MessageType:
    NEW_JOB = 1
    JOB_REPORT = 2
    NEW_MAIL = 3
    SEND_MAIL = 4
    PROFILE_SUMMARY = 5
    UPDATE_PROFILE = 6
    RESIGNATION = 7
    DIE = 8

class Message:
    def __init__(self, msgtype):
        self.type = msgtype
