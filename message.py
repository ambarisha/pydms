
class MessageType:
    NEW_JOB = 1
    JOB_REPORT = 2
    NEW_MAIL = 3
    SEND_MAIL = 4
    PROFILE_SUMMARY_REQUEST = 5
    PROFILE_SUMMARY = 6
    UPDATE_PROFILE = 7
    RESIGNATION = 8

class Message:
    def __init__(self, msgtype):
        self.type = msgtype
