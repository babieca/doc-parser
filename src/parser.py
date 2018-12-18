from gevent import monkey
monkey.patch_all()
import gevent
import logging
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

# This section at the beginning of every .py file
logger = logging.getLogger('partnerscap')
logger.info('Entered module: %s' % __name__)

def parse_pdf(f, encoding='utf-8'):

    t0 = time()
    content = {}
    
    logger.debug("Parsing PDF '{}'".format(f.get('fpath')))
    logger.debug('Gevent (init parse_pdf): {}'.
                 format(gevent.getcurrent().name))

    if 'pdf' in f.get('fext'):

        eof = subprocess.check_output(['tail', '-1', f.get('fpath')])
        if b'%%EOF' != eof and b'%%EOF\n' != eof and b'%%EOF\r\n' != eof:
            return {'error': 'EOF', 'args': f}

        fname = f.get('fname').replace(" ", "_")

        f_raw = open(f.get('fpath'), 'rb')

        with FileObjectThread(f_raw, 'rb') as pdffile:

            clean_text = ''

            t1 = time()
            logger.debug('Gevent (before textract.process): {}'.
                         format(gevent.getcurrent().name))
            try:
                text = textract.process(f.get('fpath'), encoding=encoding)

            except Exception as e:
                logger.error(("Unexpected error while parsing PDF file '{}' " +
                              "using textract. Error: ").
                              format(f.get('fpath'), str(e)))
                return {'status': 'error',
                        'error': str(e),
                        'args': f, 
                        'data': None}
            
            logger.debug('Gevent (after textract.process: {} - {}'.
                         format(gevent.getcurrent().name, time() - t1))

            text = text.decode("utf-8")
            text = ''.join(list(
                filter(lambda x: x in set(string.printable), text)))
            text = text.split('\n')

            for line in text:
                if not line and clean_text[-2:] != '\n\n':
                    clean_text += '\n'
                else:
                    min_char_len = 8
                    clean_line = utils.remove_nonsense_lines(str(line), 
                                                             min_char_len)
                    if clean_line:
                        clean_text += clean_line + '\n'
            
            if not clean_text:
                logger.error(("textract was unable to parse " + 
                              "the contents of the document '{}'").
                              format(f.get('fpath')))
                return {'status': 'error',
                        'error': 'textract unable to parse any content',
                        'args': f, 
                        'data': None}
            
            clean_text_bytes = bytes(clean_text, encoding=encoding)            
            clean_text_b64str = base64.b64encode(clean_text_bytes).decode('utf-8')
            hash_object = hashlib.sha512(clean_text_bytes)
            hex_dig = hash_object.hexdigest()

            t2 = time()
            logger.debug('Gevent (before PdfFileReader): {}'.
                         format(gevent.getcurrent().name))
            
            pdf = None
            title = ''
            numpages = -1
            
            try:
                pdf = PdfFileReader(pdffile, strict=False)
                
            except Exception as e:
                logger.error(("Unexpected error while parsing PDF file '{}' " +
                              "using PyPDF2. Error: ").
                              format(f.get('fpath'), str(e)))
            
            if pdf:
                try:
                    info = pdf.getDocumentInfo()
                    title = info.title
                except:
                    logger.error(("PyPDF2 cannot read the title " +
                                  " of document '{}'").format(f.get('fpath')))
                
                try:
                    numpages = pdf.getNumPages()
                except:
                    logger.error(("PyPDF2 cannot read the number of pages " +
                                  " of document '{}'").format(f.get('fpath')))

            logger.debug('Gevent (after PdfFileReader: {} - {}'.
                         format(gevent.getcurrent().name, time() - t2))
            
            content = {
                'info': {
                    'path': f.get('fpath', ''),
                    'directory': f.get('dir', ''),
                    'filename': f.get('fname', ''),
                    'extension': f.get('fext', ''),
                    'content_sha512_hex': hex_dig,
                    'pages': numpages
                },
                'title': title,
                'content': clean_text,
                'content_base64': clean_text_b64str,
                'summary': '',
                'created': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            logger.debug('Gevent (end parse_pdf): {} - {}'.
                         format(gevent.getcurrent().name, time() - t0))
            
            if not content:
                logger.error("Empty content for '{}'".format(f.get('fpath')))
                
            return {'status': 'ok',
                    'error': None,
                    'args': f, 
                    'data': content}

