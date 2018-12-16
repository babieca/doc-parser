from gevent import monkey
monkey.patch_all()
import logging
import gevent
import string
import base64
import hashlib
import subprocess
from gevent.fileobject import FileObjectThread
from PyPDF2 import PdfFileReader
import textract
from datetime import datetime
from time import time
import utils

logger = logging.getLogger('partnerscap')

def parse_pdf(f, encoding='utf-8'):

    logger.info("Parsing PDF '{}'".format(f))
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
                    clean_line = utils.remove_nonsense_lines(str(line), min_char_len)
                    if clean_line:
                        clean_text += clean_line + '\n'

            encoded = base64.b64encode(bytes(clean_text, 'utf-8'))

            t2 = time()
            logger.debug('Gevent (before PdfFileReader): {}'.format(gevent.getcurrent().name))
            pdf = PdfFileReader(pdffile, strict=False)
            logger.debug('Gevent (after PdfFileReader: {} - {}'.format(gevent.getcurrent().name, time() - t2))
            info = pdf.getDocumentInfo()
            
            hash_object = hashlib.sha512(str.encode(clean_text))
            hex_dig = hash_object.hexdigest()
            
            content = {
                'info': {
                    'path': f.get('fpath', ''),
                    'directory': f.get('dir', ''),
                    'filename': f.get('fname', ''),
                    'extension': f.get('fext', ''),
                    'hash_content': hex_dig,
                    'pages': pdf.getNumPages()
                },
                'title': info.title,
                'content': clean_text,
                'summary': '',
                'created': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            logger.debug('Gevent (end parse_pdf): {} - {}'.format(gevent.getcurrent().name, time() - t0))
            return content

