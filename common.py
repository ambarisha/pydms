from sys import stderr
import signal

DMS_UDS_PATH = "dms.uds"

sigint = False
sigsegv = False
sigterm = False

def set_signal_flag(sig, frame):
    global sigint, sigsegv, sigterm
    if sig == signal.SIGINT:
        sigint = True
    elif sig == signal.SIGSEGV:
        sigsegv = True
    elif sig == signal.SIGTERM:
        sigterm = True

def setup_signal_recording():
    signal.signal(signal.SIGABRT, signal.SIG_IGN)
    signal.signal(signal.SIGFPE, signal.SIG_IGN)
    signal.signal(signal.SIGINT, set_signal_flag)
    signal.signal(signal.SIGILL, signal.SIG_IGN)
    signal.signal(signal.SIGSEGV, set_signal_flag)
    signal.signal(signal.SIGTERM, set_signal_flag)
    return

def fatal(msg):
    print msg
    exit()

def log(msg):
    stderr.write(msg + '\n')
