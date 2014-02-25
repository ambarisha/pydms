
class MessageType:
    NEW_JOB = 1
    JOB_REPORT = 2
    SIGNAL = 3
    SEND_MAIL = 4
    RESIGNATION = 5
    REQUEST = 6
    DIE = 7
    RELEIVE = 8
    FINISHED = 9
    AUTH_RESPONSE = 10

class Message:
    def __init__(self, msgtype):
        self.type = msgtype
