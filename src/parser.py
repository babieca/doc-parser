from gevent import monkey
monkey.patch_all()
import gevent
import os
import re
import base64
import hashlib
import subprocess
import uuid
import shutil
from gevent.fileobject import FileObjectThread
from subprocess import Popen, PIPE
import textract
from pdf2image import convert_from_path, convert_from_bytes
from pdf2image.exceptions import (
    PDFInfoNotInstalledError,
    PDFPageCountError,
    PDFSyntaxError
)
from datetime import datetime
from time import time
import tempfile
import utils
from control import logger, decfun
from text_summary import text_summary


def regex_srch(text, search):
    match = ''
    try:
        match = re.search(r'(?<='+search+').*', text).group().strip()
    except:
        return None
    else:
        return utils.remove_non_printable_chars(match)


def get_pdfinfo(pdf_path, userpw=None):
    try:
        if userpw is not None:
            proc = Popen(["pdfinfo", pdf_path, '-upw', userpw],
                         stdout=PIPE, stderr=PIPE)
        else:
            proc = Popen(["pdfinfo", pdf_path], stdout=PIPE, stderr=PIPE)

        out, err = proc.communicate()
    except:
        raise PDFInfoNotInstalledError(('Unable to get page count. ' +
                                        'Is poppler installed and in PATH?'))

    try:

        data = out.decode("utf8", "ignore")
        pdfinfo = {
            'title': regex_srch(data, 'Title:'),
            'creator': regex_srch(data, 'Creator:'),
            'producer': regex_srch(data, 'Producer:'),
            'tragged': regex_srch(data, 'Tagged:'),
            'user_properties': regex_srch(data, 'UserProperties:'),
            'suspects': regex_srch(data, 'Suspects:'),
            'from': regex_srch(data, 'Form:'),
            'javascript': regex_srch(data, 'JavaScript:'),
            'pages': utils.input2num(regex_srch(data, 'Pages:')),
            'encripted': regex_srch(data, 'Encrypted:'),
            'page_size': regex_srch(data, 'Page size:'),
            'page_rot': regex_srch(data, 'Page rot:'),
            'file_size': regex_srch(data, 'File size:'),
            'optimized': regex_srch(data, 'Optimized:'),
            'pdf_version': regex_srch(data, 'PDF version:'),
            'creation_date': regex_srch(data, 'CreationDate:'),
            'author': regex_srch(data, 'Author:')
        }
        return pdfinfo
    except:
        raise PDFPageCountError('Unable to get pdf info. %s' % err.decode("utf8", "ignore"))


@decfun
def parse_pdf(root, file_name, file_extension, folder='', encoding='utf-8'):

    t0 = time()
    content = {}
    file_path = os.path.join(root, folder, file_name+file_extension)
    logger.debug('Gevent (init parse_pdf): {}. File: {}'.
                 format(gevent.getcurrent().name, file_path))

    status = 'error'
    clean_text = ''
    content = {}
    
    if file_extension != '.pdf':
        logger.error("File extension of '{}' is not '.pdf'".format(file_path))

    else:

        eof = subprocess.check_output(['tail', '-n', '1', file_path])
        # %%EOF, %%EOF\n, %%EOF\r, %%EOF\r\n
        eof = eof.replace(b'\r', b'')
        eof = eof.replace(b'\n', b'')
        if (b'%%EOF' in eof[-4:]):
            logger.error("Error reading EOF bytes '{}' from '{}'".
                    format(eof.decode('utf-8'), file_path))
        else:

            f_raw = open(file_path, 'rb')

            with FileObjectThread(f_raw, 'rb') as pdffile:

                t1 = time()
                
                pdfinfo = get_pdfinfo(file_path)
                numpages = pdfinfo.get('pages', -1)
                
                logger.debug('Gevent (before textract.process): {}'.
                             format(gevent.getcurrent().name))
                try:
                    text = textract.process(file_path, encoding=encoding)

                except:
                    logger.error(("Unexpected error while parsing PDF file_path '{}' " +
                                  "using textract").format(file_path))
                    return {'status': status, 'args': file_path, 'data': content}

                logger.debug('Gevent (after textract.process: {} - {}'.
                             format(gevent.getcurrent().name, time() - t1))

                text = text.decode("utf-8")
                text = utils.remove_non_printable_chars(text)
                text = text.split('\n')

                for line in text:
                    if not line and clean_text[-2:] != '\n\n':
                        clean_text += '\n'
                    else:
                        if text.count(line) <= max(numpages-10, 4):
                            #remove extra spaces
                            clean_line = re.sub(r'\s+', ' ', line)
                            clean_line = utils.remove_nonsense_lines(str(clean_line), 6)
                            if clean_line:
                                clean_text += clean_line + '\n'

                if not clean_text:
                    logger.error(("textract was unable to parse " +
                                  "the contents of the document '{}'").
                                  format(file_path))
                    return {'status': status, 'args': file_path, 'data': content}

                summary = text_summary(clean_text)
                clean_text_bytes = bytes(clean_text, encoding=encoding)
                clean_text_b64str = base64.b64encode(clean_text_bytes).decode('utf-8')
                hash_object = hashlib.sha512(clean_text_bytes)
                hex_dig = hash_object.hexdigest()

                content = {
                    'meta': {
                        'dir_root': root,
                        'folder_file': folder,
                        'filename': file_name,
                        'extension': file_extension,
                        'content_sha512_hex': hex_dig,
                        **pdfinfo
                    },
                    'content': clean_text,
                    'content_base64': clean_text_b64str,
                    'summary': summary,
                    'created': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

                logger.debug('Gevent (end parse_pdf): {} - {}'.
                             format(gevent.getcurrent().name, time() - t0))

                if not content:
                    status = 'error'
                    logger.error("Empty content for '{}'".format(file_path))
                else:
                    status = 'ok'

    return {'status': status, 'args': file_path, 'data': content}


@decfun
def parse_pdf2img(filename, folder_img):
    try:
        with tempfile.TemporaryDirectory() as tmppath:
            images = convert_from_path(filename, dpi=80, fmt='jpeg', strict=False,
                           last_page=10, output_folder=tmppath)

            utils.create_directory(folder_img)
            files = os.listdir(tmppath)
            for file in files:
                src = os.path.join(tmppath, file)
                shutil.move(src, folder_img)
        return True
    except:
        logger.error(("pdf2image could not convert " +
                          " the document '{}'").format(filename))
        return False

