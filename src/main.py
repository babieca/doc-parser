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
def main(es_addr, es_port, dir_root, dir_processed, dir_error):

    if not os.path.isabs(dir_root):
        dir_root = os.path.abspath(dir_root)
    if not os.path.isabs(dir_processed):
        dir_processed = os.path.abspath(dir_processed)
    if not os.path.isabs(dir_error):
        dir_error = os.path.abspath(dir_error)

    utils.create_directory(dir_processed)
    utils.create_directory(dir_error)

    status_time = 10

    utils.replace_recursively(dir_root)
    files = utils.files_in_dir_recursively(dir_root, '.pdf')

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
            g1.append(gevent.spawn(parser.parse_pdf,
                                   root=file.get('root'),
                                   folder=file.get('folder'),
                                   file_name=file.get('fname'),
                                   file_extension=file.get('fext')))

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
            data = result.get('data')
           
            dir_from = data.get('meta', {}).get('dir_root')
            folder_doc = data.get('meta', {}).get('folder_file', '')
            filename = data.get('meta', {}).get('filename')
            file_extension = data.get('meta', {}).get('extension')
            
            full_path2file = os.path.join(dir_from, folder_doc, filename + file_extension)
            
            if result.get('status') == 'error':
                dir_to = os.path.join(dir_error, folder_doc)
                utils.move_to(filename, dir_to)

            else:
                to_search = data.get('meta', {}).get('content_sha512_hex')
                query = query_builder(
                    field="meta.content_sha512_hex",
                    to_search=to_search)

                if es.search('files', query)['hits']['total'] == 0:
                    tm = datetime.now().strftime("%Y%m%d_%H%M%S")
                    
                    folder_img = os.path.join('images', folder_doc, filename[:filename.find('.')] + tm)
                    dir_to_img = os.path.join(dir_processed, folder_img)
                    
                    folder_file = os.path.join('files', folder_doc)
                    data['meta']['folder_file'] = folder_file
                    
                    if parser.parse_pdf2img(full_path2file, dir_to_img):
                        data['meta']['dir_root'] = dir_processed
                        data['meta']['folder_img'] = folder_img
                    else:
                        data['meta']['folder_img'] = ''

                    g2.append(gevent.spawn(es.store_record, 'files', '_doc', data))
                    foo = [g.link_exception(on_exception) for g in g2]
                    
                    dir_to = os.path.join(dir_processed, folder_file)
                    utils.move_to(full_path2file, dir_to)
                else:
                    logger.info("File '{}' already in the database. Skipped".
                                format(data.get('meta', {}).get('path_file')))
                
                

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
        args=(es_addr, es_port, dir_root, dir_processed, dir_error))

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
