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
def main(es_addr, es_port, dir_root, dir_processed, dir_error, folder_images):

    if not os.path.isabs(dir_root):
        dir_root = os.path.abspath(dir_root)
    if not os.path.isabs(dir_processed):
        dir_processed = os.path.abspath(dir_processed)
    if not os.path.isabs(dir_error):
        dir_error = os.path.abspath(dir_error)
        
    utils.create_directory(dir_processed)
    utils.create_directory(dir_error)

    status_time = 10
    
    files = utils.list_files_recursively(dir_root, '.pdf')

    if len(files) == 0:
        logger.info("Directory '{}' is empty".format(dir_root))
        return
    
    chunks_size = 10
    chunk_of_files = list(utils.chunks(files, chunks_size))

    round = 1
    for cfs in chunk_of_files:
        logger.info('Round: {}/{}, Files (round/total): {}/{}'.
                    format(round, len(chunk_of_files), len(cfs), len(files)))
        g1 = []
        for file in cfs:
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
            n = 0
            filename = result.get('args')
            dir_from = os.path.dirname(filename)
            file = utils.path_leaf(filename)
            while n<min(len(dir_root), len(dir_from)) and dir_root[n] == dir_from[n]: n+=1
            subfolder = dir_from[n:]
            if subfolder.startswith('/'): subfolder = subfolder[1:]
            
            if result.get('status') == 'error':
                dir_to = os.path.join(dir_error, subfolder)
                utils.move_to(filename, dir_to)
                continue
            else:
                dir_to = os.path.join(dir_processed, subfolder)
            
            data = result.get('data')
    
            to_search = data.get('meta', {}).get('content_sha512_hex')
            query = query_builder(
                field="meta.content_sha512_hex",
                to_search=to_search)
            
            if es.search('files', query)['hits']['total'] == 0:
                dir_to_img = os.path.join(
                    dir_to, folder_images, file[:file.find('.')])
                
                if parser.parse_pdf2img(filename, dir_to_img):
                    data['meta']['path_file'] = dir_to
                    data['meta']['path_img'] = dir_to_img
                else:
                    data['meta']['path_img'] = ''
            
                g2.append(gevent.spawn(es.store_record, 'files', '_doc', data))
                foo = [g.link_exception(on_exception) for g in g2]
                
            else:
                logger.info("File '{}' already in the database. Skipped".
                            format(data.get('meta', {}).get('path_file')))
            
            utils.move_to(filename, dir_to)
        
        if MONITOR_STATUS: monitor_greenlet_status(g2, status_time)
        
        gevent.joinall(g2)
        round +=1
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
    interval = config.get('freq_min', 5)
    
    if not config_app:
        logger.error('Missing: config > app')
        sys.exit(1)
        
    if not config_es:
        logger.error('Missing: config > elasticsearch')
        sys.exit(1)
        
    es_addr = config_es.get('host', '127.0.01')
    es_port = config_es.get('port', 9200)
    dir_root = config_app.get('dir_root')
    dir_processed = config_app.get('dir_processed')
    dir_error = config_app.get('dir_errors')
    folder_images = config_app.get('folder_images', 'images')
    
    if not dir_root:
        logger.error('Missing: config > app > dir_root')
        sys.exit(1)
    if not dir_processed:
        logger.error('Missing: config > app > dir_processed')
        sys.exit(1)
    if not dir_error:
        logger.error('Missing: config > app > dir_error')
        sys.exit(1)

    es_init(es_addr, es_port)
    
    scheduler.add_job(main, 'interval', minutes=interval, name='main_job',
        next_run_time=datetime.now(), replace_existing=True,
        max_instances=1,
        args=(es_addr, es_port, dir_root, dir_processed, dir_error, folder_images))
    
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
