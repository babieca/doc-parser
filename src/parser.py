from gevent import monkey
monkey.patch_all()
import gevent
import os
import logging
import string
import base64
import hashlib
import subprocess
import uuid
from gevent.fileobject import FileObjectThread
import textract
from PyPDF2 import PdfFileReader
from pdf2image import convert_from_path, convert_from_bytes
from pdf2image.exceptions import (
    PDFInfoNotInstalledError,
    PDFPageCountError,
    PDFSyntaxError
)
from datetime import datetime
from time import time
import utils

# This section at the beginning of every .py file
logger = logging.getLogger('partnerscap')
logger.info('Entered module: %s' % __name__)


###################################################
# At the beginning of every .py file in the project
DECORATOR = True
LOGGERNAME = 'partnerscap'

def logFunCalls(fn):
    def wrapper(*args, **kwargs):
        id = str(uuid.uuid4())[:8]
        fname = fn.__name__
        logger = logging.getLogger(LOGGERNAME)
        logger.info("[  in  ]  '{fname}' ({id})".
                    format(fname=fname, id=id))
        
        for arg in args:
            logger.info('args ({id}): {arg}'.
                        format(id=id, arg=arg))
        for key, value in kwargs.items():
            logger.info("kwargs ({id}): {key}:{value}".
                        format(id=id, key=key, value=value))
            
        t1 = time()
        out = fn(*args, **kwargs)
        
        logger.info("[ out  ]  '{fname}' ({id}) {tm} secs.".
                    format(fname=fname, id=id, tm=round(time()-t1, 4)))
        # Return the return value
        return out
    return wrapper


def decfun(f):
    if DECORATOR:
        return logFunCalls(f)
    else:
        return f
###################################################

@decfun
def parse_pdf(file, encoding='utf-8'):

    t0 = time()
    content = {}
    
    logger.debug('Gevent (init parse_pdf): {}. File: {}'.
                 format(gevent.getcurrent().name, file))
    
    file_path = os.path.dirname(file)
    filename_w_extension = os.path.basename(file)
    filename, file_extension = os.path.splitext(filename_w_extension)
    
    status = 'error'
    clean_text = ''
    content = {}
    pdf = None
    title = ''
    numpages = -1

    if file_extension != '.pdf':
        logger.error("File extension of '{}' is not '.pdf'".format(file))
        
    else:
        
        eof = subprocess.check_output(['tail', '-1', file])
        if b'%%EOF' != eof and b'%%EOF\n' != eof and b'%%EOF\r\n' != eof:
            logger.error("Error reading EOF bytes of '{}'".format(file))
    
        else:
            
            f_raw = open(file, 'rb')
        
            with FileObjectThread(f_raw, 'rb') as pdffile:
        
                t1 = time()
                logger.debug('Gevent (before textract.process): {}'.
                             format(gevent.getcurrent().name))
                try:
                    text = textract.process(file, encoding=encoding)
        
                except:
                    logger.error(("Unexpected error while parsing PDF file '{}' " +
                                  "using textract").format(file))
                    return {'status': status, 'args': file, 'data': content}
                
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
                        clean_line = utils.remove_nonsense_lines(str(line), 6)
                        if clean_line:
                            clean_text += clean_line + '\n'
                
                if not clean_text:
                    logger.error(("textract was unable to parse " + 
                                  "the contents of the document '{}'").
                                  format(file))
                    return {'status': status, 'args': file, 'data': content}
                
                clean_text_bytes = bytes(clean_text, encoding=encoding)            
                clean_text_b64str = base64.b64encode(clean_text_bytes).decode('utf-8')
                hash_object = hashlib.sha512(clean_text_bytes)
                hex_dig = hash_object.hexdigest()
        
                t2 = time()
                logger.debug('Gevent (before PdfFileReader): {}'.
                             format(gevent.getcurrent().name))
                
                try:
                    pdf = PdfFileReader(pdffile, strict=False)
                    
                except Exception as e:
                    logger.error(("Unexpected error while parsing PDF file '{}' " +
                                  "using PyPDF2. Error: ").
                                  format(file, str(e)))
                
                if pdf:
                    try:
                        info = pdf.getDocumentInfo()
                        title = info.title
                    except:
                        logger.error(("PyPDF2 cannot read the title " +
                                      " of document '{}'").format(file))
                    
                    try:
                        numpages = pdf.getNumPages()
                    except:
                        logger.error(("PyPDF2 cannot read the number of pages " +
                                      " of document '{}'").format(file))
                
                logger.debug('Gevent (after PdfFileReader: {} - {}'.
                             format(gevent.getcurrent().name, time() - t2))
                
                content = {
                    'meta': {
                        'path_file': file_path,
                        'filename': filename,
                        'extension': file_extension,
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
                    status = 'error'
                    logger.error("Empty content for '{}'".format(file))
                else:
                    status = 'ok'
        
    return {'status': status, 'args': file, 'data': content}


@decfun
def parse_pdf2img(content):

    folder_path = content.get('meta',{}).get('path_file')
    filename = content.get('meta',{}).get('filename')
    folder_img = os.path.join(folder_path, 'images', filename)
    file_path = os.path.join(folder_path, filename)
    
    utils.create_directory(folder_img)
    
    if file_path and filename:
        try:
            images = convert_from_path(file_path, #dpi=80, fmt='jpeg', strict=False,
                           output_folder=folder_img)

            content['meta']['path_file'] = os.path.join(folder_path, 'processed')
            content['meta']['path_img'] = folder_img
        except:
            logger.error(("pdf2image could not convert " +
                              " the document '{}'").format(filename))
            content['meta']['path_img'] = ''
    else:
        raise ValueError('Error parsing pdf to images')
    
    return content
