from gevent import monkey
monkey.patch_all()
import gevent
import sys
import uuid
import logging
from time import time
from config import config
import utils

###################################
# CONTROL

DECORATOR = True
LOGGERNAME = config['app']['app_name']

###################################

loglevel = logging.INFO
logfile_path = config['app']['logfile']

formatter = logging.Formatter(
    ('%(asctime)s.%(msecs)03d - %(levelname)s - ' +
    '[%(filename)s:%(lineno)d] #  %(message)s'),
    '%Y-%m-%d:%H:%M:%S')

logger = logging.getLogger(LOGGERNAME)
logger.setLevel(loglevel)
logger.propagate = False

sHandler = logging.StreamHandler(stream=sys.stdout)
sHandler.setLevel(loglevel)
sHandler.setFormatter(formatter)

fHandler = logging.FileHandler(logfile_path, encoding='utf-8') #, mode='w')
fHandler.setLevel(loglevel)
fHandler.setFormatter(formatter)

logger.addHandler(sHandler)
logger.addHandler(fHandler)


def logFunCalls(fn):
    def wrapper(*args, **kwargs):
        id = str(uuid.uuid4())[:8]
        fname = fn.__name__
        logger = logging.getLogger(LOGGERNAME)

        arg = '; '.join(str(arg) for arg in args)
        logger.info("in: '{fname}' ({id})  [ args: {arg} ]".
                    format(fname=fname, id=id,
                           arg=utils.cut_line(arg, 80)))
        for key, value in kwargs.items():
            logger.info("in: '{fname}' ({id})  [ kwargs: {key}:{value} ]".
                        format(fname=fname, id=id,
                               key=utils.cut_line(key, 20),
                               value=utils.cut_line(value, 80)))

        t1 = time()
        out = fn(*args, **kwargs)

        logger.info("out: '{fname}' ({id}) {tm} secs.".
                    format(fname=fname, id=id, tm=round(time()-t1, 4)))
        # Return the return value
        return out
    return wrapper


def decfun(f):
    if DECORATOR:
        return logFunCalls(f)
    else:
        return f



def _get_status(greenlets):
    total = running = completed = 0
    succeeded = queued = failed = 0

    for g in greenlets:
        total += 1
        if bool(g):
            running += 1
        else:
            if g.ready():
                completed += 1
                if g.successful():
                    succeeded += 1
                else:
                    failed += 1
            else:
                queued += 1

    assert queued == total - completed - running
    assert failed == completed - succeeded

    result = {'Total': total, 'Running': running, 'Completed': completed,
              'Succeeded': succeeded, 'Queued': queued, 'Failed': failed }
    return result


def monitor_greenlet_status(greenlets, sec=5):
    session = str(uuid.uuid4())[:8]
    while True:
        status = _get_status(greenlets)
        logger.info('Session: {} >> {}'.format(session, status))
        if status['Total'] == status['Completed']:
            return
        gevent.sleep(sec)


