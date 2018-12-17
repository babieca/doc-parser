from gevent import monkey
monkey.patch_all()
import gevent
import os
import sys
import logging
import json
import hashlib
import string
import uuid

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


def get_greenlet_status(greenlets, sec=5):
    session = str(uuid.uuid4())
    while True:
        status = _get_status(greenlets)
        logger.info('Session: {} >> {}'.format(session, status))
        if status['Total'] == status['Completed']:
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


def files_in_dir(dir):

    if not os.path.isdir(dir):
        raise ValueError("[  OS  ]  Directory does not exist.")

    filesdic = []
    dirpath = os.path.abspath(dir)

    for f in os.listdir(dirpath):
        fpath = os.path.join(dirpath, f)
        if os.path.isfile(fpath):
            filesdic.append({'fpath': fpath,    # directory + file name + extension
                             'dir': dirpath,                        # directory
                             'fname': f.split('.')[0],              # file name
                             'fext': os.path.splitext(fpath)[1]})   # extension
    return filesdic


def remove_nonsense_lines(line, min=4):
    counter = 0
    for c in line:
        if c in string.printable:
            counter += 1
        if counter >= min:
            return line
    return False


def create_directory(dirname):
    if not os.path.exists(dirname):
        try:
            os.makedirs(dirname)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise ValueError(("The directory '{}' was created " +
                    "between the os.path.exists and the os.makedirs").
                    format(dirname))
    return True

