from gevent import monkey
monkey.patch_all()
import gevent
import os
import re
import sys
import errno
import json
import hashlib
import string
import uuid
from datetime import datetime
from control import logger, decfun


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
        tm = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'[:-3])
        sys.stdout.write('{tm} # {question} {prompt}: '.format(tm = tm, question=question, prompt=prompt))
        choice = input().lower()
        if isinstance(default, bool) or choice in valid:
            return valid.get(choice, default)
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


def hashfile(fpath):

    blocksize = 65536
    hasher = sha256()
    with open(fpath, 'rb') as f:
        buf = f.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(blocksize)
    return hasher.hexdigest()


def files(dir, extension=None):

    if not os.path.isdir(dir):
        raise ValueError("[  OS  ]  Directory does not exist.")

    filesdic = []
    dirpath = os.path.abspath(dir)

    for f in os.listdir(dirpath):
        fpath = os.path.join(dirpath, f)
        if os.path.isfile(fpath):
            if not extension or (extension and f.endswith(extension)):
                filesdic.append({
                    'fpath': dirpath,                      # directory
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


def folder_tree_structure(dir_root):
    if not dir_root or type(dir_root) is not str:
        raise ValueError('Root directory must be an absolute or relative path')
    if not os.path.isdir(dir_root):
        raise ValueError('Root directory does not exist')
    
    if not os.path.isabs(dir_root):
        dir_root = os.path.abspath(dir_root)
    
    dir_pdfs = os.path.join(dir_root, 'pdfs')
    dir_err = os.path.join(dir_root, 'errors')
    
    create_directory(dir_pdfs)
    create_directory(dir_err)
    
    return (dir_pdfs, dir_err)


def move_to(src_file, dst_folder):

    if not src_file or type(src_file) is not str:
        raise ValueError('Error reading source file')
    if not dst_folder or type(dst_folder) is not str:
        raise ValueError('Error reading destination path')
    if not os.path.exists(src_file):
        raise ValueError("File '{}' do not exist".format(src))
    if not os.path.exists(dst_folder):
        create_directory(dst_folder)
    
    file_w_extension = os.path.basename(src_file)
    dst_file = os.path.join(dst_folder, file_w_extension)
    
    os.rename(src_file, dst_file)


def input2num(iput):

        regnum = re.compile("^(?=.*?\d)\d*[.,]?\d*$")
        if iput:
            if iput.isdigit():
                return float(iput)

            oput = iput.replace(",", "")
            if regnum.match(oput):
                return float(oput)
        return -1


def cut_line(line, maxchar=80):
    if not line:
        raise ValueError("input line can not be empty")
    if type(line) is not str:
        try:
            line = str(line)
        except:
            raise ValueError("line must be a str, not a '{}' type".
                             format(type(line)))
    
    if len(line) > (maxchar-3):
        return line[:maxchar-3] + '... '
    else:
        return line


