#!/usr/bin/env python3
from gevent import monkey
monkey.patch_all()
import os
import re
import sys
import string
import subprocess
import base64
import logging
import gevent
import textract
from time import time
from gevent.fileobject import FileObjectThread
from datetime import datetime
from PyPDF2 import PdfFileReader
from apscheduler.schedulers.gevent import GeventScheduler
from esdb import ES
import utils
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


def parse_pdf(f, encoding='utf-8'):

    t0 = time()

    logger.debug('Gevent (init parse_pdf): {}'.format(gevent.getcurrent().name))

    content = {}

    if 'pdf' in f.get('fext'):

        eof = subprocess.check_output(['tail', '-1', f.get('fpath')])
        if b'%%EOF' != eof and b'%%EOF\n' != eof and b'%%EOF\r\n' != eof:
            return None

        fname = f.get('fname').replace(" ", "_")

        f_raw = open(f.get('fpath'), 'rb')

        with FileObjectThread(f_raw, 'rb') as pdffile:

            clean_text = ''

            t1 = time()
            logger.debug('Gevent (before textract.process): {}'.format(gevent.getcurrent().name))
            text = textract.process(f.get('fpath'), encoding=encoding)
            logger.debug('Gevent (after textract.process: {} - {}'.format(gevent.getcurrent().name, time() - t1))

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

            # ftext.write(clean_text)
            encoded = base64.b64encode(bytes(clean_text, 'utf-8'))

            t2 = time()
            logger.debug('Gevent (before PdfFileReader): {}'.format(gevent.getcurrent().name))
            pdf = PdfFileReader(pdffile, strict=False)
            logger.debug('Gevent (after PdfFileReader: {} - {}'.format(gevent.getcurrent().name, time() - t2))
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

            logger.debug('Gevent (end parse_pdf): {} - {}'.format(gevent.getcurrent().name, time() - t0))
            return content


def main(es_addr, es_port, es_index, es_docs, es_mapping, files_dir):

    es = ES(es_addr, es_port)
    es.connect()

    es.secure_delete_index(es_index)
    
    if es.create_index(es_index, es_mapping):

        allfiles = get_files_in_dir(files_dir)
        
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
        
        allfiles = [f for f in allfiles
                    if es.search(es_index, content.format(fname=f.get('fname')))['hits']['total'] == 0]
        
        if len(allfiles) == 0:
            logger.info('No documents to index')
            return
        
        question = 'Index {} documents in index {}?'.format(len(allfiles), es_index)
        if utils.query_yes_no(question):
    
            g1 = [gevent.spawn(parse_pdf, f) for f in allfiles]
            utils.get_greenlet_status(g1, 5)
            gevent.joinall(g1)
        
            g2 = [gevent.spawn(es.store_record, es_index, es_docs, g.value) for g in g1]
            utils.get_greenlet_status(g2, 5)
            gevent.joinall(g2)
    

if __name__ == '__main__':

    es_addr = config_es.get('host', '127.0.01')
    es_port = config_es.get('port', 9200)
    es_index = config_es.get('index', '')
    es_docs = config_es.get('documents', '')
    es_mapping = config_es.get('mapping', '')
    files_dir = './data/gutenberg'

    main(es_addr, es_port, es_index, es_docs, es_mapping, files_dir)
    
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
    
    sys.exit(0)
