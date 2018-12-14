#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from gevent import monkey
monkey.patch_all()

import os
import sys
import string
import codecs
import json
import string
import subprocess
import base64
import logging
import gevent
from time import time
from gevent.fileobject import FileObjectThread
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.client.ingest import IngestClient
import textract
from PyPDF2 import PdfFileReader

formatter = '%(asctime)s.%(msecs)03d -%(levelname)s- [%(filename)s:%(lineno)d] #  %(message)s'
logging.basicConfig(format=formatter,
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.INFO)

#https://stackoverflow.com/questions/37861279/how-to-index-a-pdf-file-in-elasticsearch-5-0-0-with-ingest-attachment-plugin?rq=1
#https://stackoverflow.com/questions/46988307/how-do-you-use-the-elasticsearch-ingest-attachment-processor-plugin-with-the-pyt

def es_connect(es_host='127.0.0.1', es_port=9200):
    es = Elasticsearch([{'host': es_host, 'port': es_port}])
    if es.ping():
        message = 'Connected to ElasticSearch on'
        logging.info('{msg} {addr}:{port}'.format(msg=message, addr=es_addr, port=es_port))
        return es
    else:
        message = 'Failed to connect to Elasticsearch on'
        logging.error('{msg} {addr}:{port}'.format(msg=message, addr=es_addr, port=es_port))
        return None


def es_create_index(es_object, es_index):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "docs": {
                # dynamic: strict --> the “doc” object will throw an exception
                # if an unknown field is encountered
                "dynamic": "strict",
                "properties": {
                    "id": { "type": "text" },
                    "info": {
                        "type": "nested",
                        "properties": {
                            "path": { "type": "keyword" },
                            "directory": { "type": "keyword" },
                            "filename": { "type": "keyword" },
                            "extension": { "type": "keyword" }
                        }
                    },
                    "title": {
                        "type": "text",
                        "fields": {
                            "english": { 
                                "type": "text",
                                "analyzer": "english"
                            }
                        }
                    },
                    "content": {
                        "type": "text",
                        "fields": {
                            "english": { 
                                "type": "text",
                                "analyzer": "english"
                            }
                        }
                    },
                    "pages": {
                        "type": "short",
                        "fields": {
                            "keyword": { "type":  "keyword" }
                        }
                    },
                    "author": {
                        "type": "nested",
                        "properties": {
                            "id": { "type": "text" },
                            "name": { "type": "text" },
                            "email": { "type": "keyword" },
                            "comment": { "type": "text" },
                            "date": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" }
                        }
                    },
                    "created": {
                        "type": "nested",
                        "properties": {
                            "id": { "type": "text" },
                            "name": { "type": "text" },
                            "comment": { "type": "text" },
                            "date": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" }
                        }
                    },
                    "comments": {
                        "type": "nested", 
                        "properties": {
                            "name": { "type": "text" },
                            "comment": { "type": "text" },
                            "age": { "type": "short" },
                            "stars": { "type": "short" },
                            "date": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" }
                        }
                    },
                    "tags": { "type":  "text" }
                }
            }
        }
    }
    try:
        if not es_object.indices.exists(es_index):
            # Ignore 400 means to ignore "Index Already Exist" error.
            res = es_object.indices.create(index=es_index, body=settings, ignore=[400, 404])
            logging.info('created index {}'.format(es_index))
            logging.info('response {}'.format(res))
        created = True
    except Exception as ex:
        logging.info('Something went wrong. {}'.format(str(ex)))
    finally:
        return created


def es_store_record(es_obj, es_index, doc_type, content):
    try:
        outcome = es_obj.index(index=es_index, doc_type=doc_type, body=content)
    except Exception as ex:
        logging.error('Error. Something went wrong storing the data. {}'.format(str(ex)))


def es_search(es_obj, es_index, to_search):
    return es_obj.search(index=es_index, body=to_search)


def get_files_in_dir(dir):
    
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


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("[  Error  ]  invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


def parse_pdf(f, encoding='utf-8'):
    
    t0 = time()
    
    logging.debug('Gevent (init parse_pdf): {}'.format(gevent.getcurrent().name))
    
    content = {}
    
    if 'pdf' in f.get('fext'):
        
        eof = subprocess.check_output(['tail', '-1', f.get('fpath')])
        if b'%%EOF' != eof and b'%%EOF\n' != eof and b'%%EOF\r\n' != eof: return None
        
        fname = f.get('fname').replace(" ", "_")
        
        f_raw = open(f.get('fpath'), 'rb')
        
        with FileObjectThread(f_raw, 'rb') as pdffile:
            
            clean_text = ''
            
            t1 = time()
            logging.debug('Gevent (before textract.process): {}'.format(gevent.getcurrent().name))
            text = textract.process(f.get('fpath'), encoding=encoding)
            logging.debug('Gevent (after textract.process: {} - {}'.format(gevent.getcurrent().name, time()-t1))
            
            text = text.decode("utf-8")
            text = ''.join(list(filter(lambda x: x in set(string.printable), text)))
            text = text.split('\n')
            
            for line in text:
                if not line and clean_text[-2:] != '\n\n':
                    clean_text += '\n'
                else:
                    min_char_len = 8
                    clean_line = remove_nonsense_lines(str(line), min_char_len)
                    if clean_line:
                        clean_text += clean_line + '\n'
            
            #ftext.write(clean_text)
            encoded = base64.b64encode(bytes(clean_text, 'utf-8'))
            
            t2 = time()
            logging.debug('Gevent (before PdfFileReader): {}'.format(gevent.getcurrent().name))
            pdf = PdfFileReader(pdffile, strict=False)
            logging.debug('Gevent (after PdfFileReader: {} - {}'.format(gevent.getcurrent().name, time()-t2))
            info = pdf.getDocumentInfo()

            content = {
                'title': info.title,
                'content': clean_text,
                'info': {
                    'path': f.get('fpath', ''),
                    'directory': f.get('dir', ''),
                    'filename': f.get('fname', ''),
                    'extension': f.get('fext', ''),
                },
                'author': {
                    'name': info.author
                },
                'created': {
                    'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                },
                'pages': pdf.getNumPages(),
            }
            
            logging.debug('Gevent (end parse_pdf): {} - {}'.format(gevent.getcurrent().name, time()-t0))
            return content


def _get_status(greenlets):
    total = 0
    running = 0
    completed = 0
    successed = 0
    yet_to_run = 0
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
                yet_to_run += 1

    assert yet_to_run == total - completed - running
    assert failed == completed - successed

    return dict(total=total,
                running=running,
                completed=completed,
                successed=successed,
                yet_to_run=yet_to_run,
                failed=failed)


def get_greenlet_status(greenlets, sec=5):
    while True:
        status = _get_status(greenlets)
        logging.info('{}'.format(status))
        if status['total'] == status['completed']:
            return
        gevent.sleep(sec)


def main(es_addr, es_port, es_index, files_dir):
    
    tasks = []
    
    es = es_connect(es_addr, es_port)
    
    if es is None: sys.exit(0)
    
    msg = 'Do you want to delete the index {}?'.format(es_index)
    if es.indices.exists(es_index):
        if query_yes_no(msg):
            res = es.indices.delete(index = es_index)
            logging.info("Deleted index {}".format(es_index))
            logging.info("{}".format(res))
        else:
            logging.info("skipped {} index...".format(es_index))
    
    es_create_index(es, es_index)
    
    allfiles = get_files_in_dir(files_dir)
    
    i = 0
    n = len(allfiles)
    
    threads = [gevent.spawn(parse_pdf, f) for f in allfiles]
    #get_greenlet_status(threads, 1)
    
    gevent.joinall(threads)
    
    for thread in threads:
        es_store_record(es, es_index, 'docs', thread.value)
     


if __name__ == '__main__':
    
    es_addr = '127.0.0.1'
    es_port = 9200
    es_index = 'gutenberg'
    
    files_dir = './data/gutenberg'
    
    main(es_addr, es_port, es_index, files_dir)
        
    logging.info('End of script.')
    sys.exit(0)




