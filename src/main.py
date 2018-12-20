#!/usr/bin/env python3
from gevent import monkey
monkey.patch_all()
import gevent
import os
import re
import sys
from datetime import datetime
from apscheduler.schedulers.gevent import GeventScheduler
from esdb import ES
import utils
import parser
from mappings import mappings
from config import config
from control import logger, decfun, monitor_greenlet_status

MONITOR_STATUS = False

def query_builder(field, to_search):
    query = '''
        {{
          "query": {{
            "nested": {{
              "path": "meta",
              "query": {{
                "term": {{
                  "{field}": {{
                    "value": "{to_search}"
                  }}
                }}
              }}
            }}
          }},
          "size": 0
        }}
        '''
    query = query.replace('\n', ' ').replace('\r', '')
    query = re.sub(' +',' ',query)
    
    query = query.format(field=field, to_search=to_search)
    
    return query


def on_exception(greenlet):
    logger.error("Greenlet '{}' died unexpectedly. Args: '{}'".
                 format(greenlet, greenlet.args))

@decfun
def main(es_addr, es_port, dir_root):
    
    dir_pdfs, dir_err = utils.folder_tree_structure(dir_root)
    
    if not os.path.isabs(dir_root):
        dir_root = os.path.abspath(dir_root)
    
    status_time = 10
    
    files = utils.files(dir_root, '.pdf')

    if len(files) == 0:
        logger.info("Directory '{}' is empty".format(dir_root))
        return
    
    logger.info('Indexing {} files'.format(len(files)))

    g1 = []
    for file in files:
        f = os.path.join(file.get('fpath'),
                         file.get('fname').replace(" ", "_") + \
                         file.get('fext'))
        g1.append(gevent.spawn(parser.parse_pdf, f))
        
    foo = [g.link_exception(on_exception) for g in g1]
    
    if MONITOR_STATUS: monitor_greenlet_status(g1, status_time)
    
    gevent.joinall(g1)

    es = ES(es_addr, es_port)
    es.connect()
    
    g2 = []
    for g in g1:
        result = g.value
        if not result: continue
        if result.get('status') == 'error':
            utils.move_to(result.get('args'), dir_err)
            continue

        data = result.get('data')

        to_search = data.get('meta', {}).get('content_sha512_hex')
        query = query_builder(
            field="meta.content_sha512_hex",
            to_search=to_search)
        
        if es.search('files', query)['hits']['total'] == 0:
            
            data = parser.parse_pdf2img(data)
        
            g2.append(gevent.spawn(es.store_record, 'files', '_doc', data))
            foo = [g.link_exception(on_exception) for g in g2]
            
        else:
            logger.info("File '{}' already in the database. Skipped".
                        format(data.get('meta', {}).get('path_file')))
            
        utils.move_to(result.get('args'), dir_pdfs)
    
    if MONITOR_STATUS: monitor_greenlet_status(g2, status_time)
    
    gevent.joinall(g2)
    
    return


def es_init(es_addr, es_port):
    
    es = ES(es_addr, es_port)
    es.connect()
    
    for idx in list(mappings.keys()):
        if not es.secure_delete_index(idx):
            logger.error("Error deleting index '{}'".format(idx))
            return

        if not es.create_index(idx, mappings.get(idx,'')):
            logger.error("Error creating index '{}'".format(idx))
            return
    
    return


if __name__ == '__main__':

    scheduler = GeventScheduler()
    config_app = config.get('app')
    config_es = config.get('elasticsearch')
    
    if not config_app:
        logger.error('Missing: config > app')
        sys.exit(1)
        
    if not config_es:
        logger.error('Missing: config > elasticsearch')
        sys.exit(1)
        
    es_addr = config_es.get('host', '127.0.01')
    es_port = config_es.get('port', 9200)
    dir_root = config_app.get('dir_root')
    
    if not dir_root:
        logger.error('Missing: config > app > dir_root')
        sys.exit(1)

    es_init(es_addr, es_port)
    
    scheduler.add_job(main, 'interval', seconds=120, name='main_job',
        next_run_time=datetime.now(), replace_existing=True,
        max_instances=1, args=(es_addr, es_port, dir_root))
    
    g = scheduler.start()  # g is the greenlet that runs the scheduler loop
    
    logger.info('Press Ctrl+{0} to exit'.
                format('Break' if os.name == 'nt' else 'C'))

    # Execution will block here
    # until Ctrl+C (Ctrl+Break on Windows) is pressed.
    try:
        g.join()
    except (KeyboardInterrupt, SystemExit):
        pass


    logger.info('[  end  ] {}'.format(__name__))
    sys.exit(0)
