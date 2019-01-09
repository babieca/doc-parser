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
import ntpath
import string
from datetime import datetime
from control import logger, decfun


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


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


def replace_recursively(walk_dir):
    regex_filter = '[^a-zA-Z0-9\.]'
    for path, folders, files in os.walk(walk_dir):
        for f in files:
            new_name = re.sub(regex_filter, '_', f)
            os.rename(os.path.join(path, f), os.path.join(path, new_name))
        for i in range(len(folders)):
            new_name = re.sub(regex_filter, '_', folders[i])
            os.rename(os.path.join(path, folders[i]), os.path.join(path, new_name))
            folders[i] = new_name


def files_in_dir_recursively(walk_dir, extension=None, exclude_dir=None):

    if walk_dir:
        if not os.path.isdir(walk_dir):
            raise ValueError("root does not exist")
        walk_dir = os.path.abspath(walk_dir)
    else:
        raise ValueError("missing walkdir")
    
    if exclude_dir:
        if not os.path.isdir(exclude_dir):
            raise ValueError("exclude_dir does not exist")
        exclude_dir = os.path.abspath(exclude_dir)
        
    filesdic = []
    
    for directory, folders, files in os.walk(walk_dir):
    
        if exclude_dir and directory.startswith(exclude_dir): continue
        
        for filename in files:
            file_path = os.path.join(directory, filename)

            if (not extension or 
                (extension and filename.endswith(extension))):
                folder = os.path.relpath(directory, walk_dir)
                if folder == '.': folder = ''
                dot = filename.find('.') if filename.find('.') >=0 else 0
                filesdic.append({
                    'root': walk_dir,           # directory
                    'folder': folder,           # subdirectory
                    'fname': filename[:dot],    # file name
                    'fext': filename[dot:]})    # extension
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
    if line:
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


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


def remove_non_printable_chars(text):
    return ''.join(list(filter(lambda x: x in set(string.printable), text)))

