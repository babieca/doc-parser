from gevent import monkey
monkey.patch_all()
import sys
import gevent
import logging
import json
import hashlib

logger = logging.getLogger('partnerscap')

def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except Exception as e:
        return False
    return True


def query_yes_no(question, default=True):

    valid = {"yes": True, "y": True,
             "no": False, "n": False}
    
    prompt = "[Y/n]" if default else "[y/N]"

    while True:
        sys.stdout.write('{question} {prompt}: '.format(question=question, prompt=prompt))
        choice = input().lower()
        if isinstance(default, bool) or choice in valid:
            return valid.get(choice, default)
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


def _get_status(greenlets):
    total = 0
    running = 0
    completed = 0
    successed = 0
    queued = 0
    failed = 0

    for g in greenlets:
        total += 1
        if bool(g):
            running += 1
        else:
            if g.ready():
                completed += 1
                if g.successful():
                    successed += 1
                else:
                    failed += 1
            else:
                queued += 1

    assert queued == total - completed - running
    assert failed == completed - successed

    return dict(total=total,
                running=running,
                completed=completed,
                successed=successed,
                queued=queued,
                failed=failed)


def get_greenlet_status(greenlets, sec=5):
    while True:
        status = _get_status(greenlets)
        logger.info('{}'.format(status))
        if status['total'] == status['completed']:
            return
        gevent.sleep(sec)


def hashfile(fpath):

    blocksize = 65536
    hasher = sha256()
    with open(fpath, 'rb') as f:
        buf = f.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(blocksize)
    return hasher.hexdigest()
