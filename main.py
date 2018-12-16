#!/usr/bin/env python3
from gevent import monkey
monkey.patch_all()
import os
import re
import sys
import errno
import logging
import gevent
from time import time
#from apscheduler.schedulers.gevent import GeventScheduler
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


def main(es_addr, es_port, src_dir):

    if not src_dir:
        raise ValueError('Source file cannot be empty')
    if not os.path.isdir(src_dir):
        raise ValueError('Source file do not exist')
    
    es = ES(es_addr, es_port)
    
    es.connect()
    
    for idx in list(mappings.keys()):
        if not es.secure_delete_index(idx):
            logger.error("Error deleting index '{}'".format(idx))
            return
        
        if not es.create_index(idx, mappings.get('mapping_'+idx,'')):
            logger.error("Error creating index '{}'".format(idx))
            return

    allfiles = utils.get_files_in_dir(src_dir)
    
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
    utils.get_greenlet_status(g1, 5)
    gevent.joinall(g1)

    g2 = [gevent.spawn(es.store_record, 'files', '_doc', g.value) for g in g1]
    utils.get_greenlet_status(g2, 5)
    gevent.joinall(g2)
     
    dst_dir = os.path.join(src_dir, 'processed')
    if not os.path.exists(dst_dir):
        try:
            os.makedirs(dst_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise ValueError(("The directory '{}' was created " +
                    "between the os.path.exists and the os.makedirs").
                    format(dst_dir))
    
    for f in allfiles:
        src = f.get('fpath')
        if not src:
            raise ValueError('Error reading source path')
        if not os.path.exists(src):
            raise ValueError("Error. File '{}' do not exist".format(src))
        if not f.get('fname'):
            raise ValueError('Error reading file name')
        if not f.get('fname'):
            raise ValueError('Error reading file extension')
        
        fname = f.get('fname') + f.get('fext')
        dst = os.path.join(dst_dir, fname)
        
        os.rename(src, dst)
        logger.info("File moved from '{}' to {}".format(src, dst))


if __name__ == '__main__':

    es_addr = config_es.get('host', '127.0.01')
    es_port = config_es.get('port', 9200)
    src_dir = config_es.get('src_dir')

    main(es_addr, es_port, src_dir)
    
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
