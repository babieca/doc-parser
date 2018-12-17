#!/usr/bin/env python3
from gevent import monkey
from textract.colors import green
monkey.patch_all()
import gevent
import os
import re
import sys
import errno
import logging
from time import time
from apscheduler.schedulers.gevent import GeventScheduler
from esdb import ES
import utils
import parser
from mappings import mappings
from config import config_es

logger = logging.getLogger('partnerscap')
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter(('%(asctime)s.%(msecs)03d - %(levelname)s - ' +
                               '[%(filename)s:%(lineno)d] #  %(message)s'),
                               '%Y-%m-%d:%H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)


# https://stackoverflow.com/questions/37861279/how-to-index-a-pdf-file-in-elasticsearch-5-0-0-with-ingest-attachment-plugin?rq=1
# https://stackoverflow.com/questions/46988307/how-do-you-use-the-elasticsearch-ingest-attachment-processor-plugin-with-the-pyt


def folder_tree_structure(dir_root):
    if not dir_root:
        raise ValueError('Source directory cannot be empty')
    if not os.path.isdir(dir_root):
        raise ValueError('Source directory do not exist')
    
    dir_new = os.path.join(dir_root, 'new')
    dir_proc = os.path.join(dir_root, 'processed')
    dir_err = os.path.join(dir_root, 'errors')
    
    if not os.path.exists(dir_new):
        raise ValueError("Directory '{}' does not exist".format(dir_new))
    
    utils.create_directory(dir_proc)
    utils.create_directory(dir_err)
    
    return (dir_new, dir_proc, dir_err)


def move_to_directory(dir_proc, data):
    src = data.get('fpath')
    if not src:
        raise ValueError('Error reading source path')
    if not os.path.exists(src):
        raise ValueError("Error. File '{}' do not exist".format(src))
    if not data.get('fname'):
        raise ValueError('Error reading file name')
    if not data.get('fname'):
        raise ValueError('Error reading file extension')
    
    fname = data.get('fname') + data.get('fext')
    dst = os.path.join(dir_proc, fname)
    
    os.rename(src, dst)
    logger.info("File moved from '{}' to {}".format(src, dst))


def on_exception(greenlet):
    logger.error("Greenlet '{}' died unexpectedly. Args: '{}'".
                 format(greenlet, greenlet.args))

def main(es_addr, es_port, dir_root):

    dir_new, dir_proc, dir_err = folder_tree_structure(dir_root)
    
    es = ES(es_addr, es_port)
    
    es.connect()
    
    for idx in list(mappings.keys()):
        if not es.secure_delete_index(idx):
            logger.error("Error deleting index '{}'".format(idx))
            return
        
        if not es.create_index(idx, mappings.get('mapping_'+idx,'')):
            logger.error("Error creating index '{}'".format(idx))
            return

    allfiles = utils.files_in_dir(dir_new)
    
    content = '''
    {{
      "query": {{
        "nested": {{
          "path": "info",
          "query": {{
            "term": {{
              "info.hash_content": {{
                "value": "{fname}"
              }}
            }}
          }}
        }}
      }},
      "size": 0
    }}
    '''
    content = content.replace('\n', ' ').replace('\r', '')
    content = re.sub(' +',' ',content)

    #allfiles = [f for f in allfiles
    #            if es.search(es_index, content.format(fname=f.get('fname')))['hits']['total'] == 0]
    
    if len(allfiles) == 0:
        logger.info('No documents to index')
        return
    
    logger.info('Indexing {} files'.format(len(allfiles)))

    g1 = [gevent.spawn(parser.parse_pdf, f) for f in allfiles]
    foo = [g.link_exception(on_exception) for g in g1]
    utils.get_greenlet_status(g1, 5)
    gevent.joinall(g1)

    g2 = []
    for g in g1:
        if not g.value: continue
        data = g.value
        if data.get('status') == 'error':
            move_to_directory(dir_err, data.get('args'))
            continue
        
        data = data.get('data')
        g2.append(gevent.spawn(es.store_record, 'files', '_doc', data))
        foo = [g.link_exception(on_exception) for g in g2]
        move_to_directory(dir_proc, data.get('args'))

    utils.get_greenlet_status(g2, 5)
    gevent.joinall(g2)


if __name__ == '__main__':

    es_addr = config_es.get('host', '127.0.01')
    es_port = config_es.get('port', 9200)
    dir_root = config_es.get('dir_root')

    main(es_addr, es_port, dir_root)
    
    '''
    # https://github.com/agronholm/apscheduler/blob/master/examples/schedulers/gevent_.py
    scheduler = GeventScheduler()
    scheduler.add_job(tick, 'interval', seconds=300)
    g = scheduler.start()  # g is the greenlet that runs the scheduler loop
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
    try:
        g.join()
    except (KeyboardInterrupt, SystemExit):
        pass
    '''
    logger.info('End of script')
    sys.exit(0)
