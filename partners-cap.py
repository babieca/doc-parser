#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import sys
import string
import codecs
import json
import string
import subprocess
import base64
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.client.ingest import IngestClient
import textract
from PyPDF2 import PdfFileReader

#https://stackoverflow.com/questions/37861279/how-to-index-a-pdf-file-in-elasticsearch-5-0-0-with-ingest-attachment-plugin?rq=1
#https://stackoverflow.com/questions/46988307/how-do-you-use-the-elasticsearch-ingest-attachment-processor-plugin-with-the-pyt

def es_connect(host='localhost', port=9200):
    _es = Elasticsearch([{'host': host, 'port': port}])
    if _es.ping():
        print('Connect to ElasticSearch on {}:{}'.format(host, port))
    else:
        print('It could not connect to ElasticSearch!')
    return _es


def es_create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "docs": {
                # dynamic: strict --> the “doc” object will throw an exception
                # if an unknown field is encountered
                "dynamic": "strict",
                "properties": {
                    "id": { "type": "text" },
                    "info": {
                        "type": "nested",
                        "properties": {
                            "path": { "type": "keyword" },
                            "directory": { "type": "keyword" },
                            "filename": { "type": "keyword" },
                            "extension": { "type": "keyword" }
                        }
                    },
                    "title": {
                        "type": "text",
                        "fields": {
                            "english": { 
                                "type": "text",
                                "analyzer": "english"
                            }
                        }
                    },
                    "content": {
                        "type": "text",
                        "fields": {
                            "english": { 
                                "type": "text",
                                "analyzer": "english"
                            }
                        }
                    },
                    "pages": {
                        "type": "short",
                        "fields": {
                            "keyword": { "type":  "keyword" }
                        }
                    },
                    "author": {
                        "type": "nested",
                        "properties": {
                            "id": { "type": "text" },
                            "name": { "type": "text" },
                            "email": { "type": "keyword" },
                            "comment": { "type": "text" },
                            "date": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" }
                        }
                    },
                    "created": {
                        "type": "nested",
                        "properties": {
                            "id": { "type": "text" },
                            "name": { "type": "text" },
                            "comment": { "type": "text" },
                            "date": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" }
                        }
                    },
                    "comments": {
                        "type": "nested", 
                        "properties": {
                            "name": { "type": "text" },
                            "comment": { "type": "text" },
                            "age": { "type": "short" },
                            "stars": { "type": "short" },
                            "date": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" }
                        }
                    },
                    "tags": { "type":  "text" }
                }
            }
        }
    }
    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            res = es_object.indices.create(index=index_name, body=settings, ignore=[400, 404])
            print('create_index[res]: {}'.format(res))
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created

def es_store_record(es_obj, index_name, doc_type, record):
    try:
        outcome = es_obj.index(index=index_name, doc_type=doc_type, body=record)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))

def es_search(es_obj, index_name, to_search):
    return es_obj.search(index=index_name, body=to_search)

def get_files_in_dir(dir):
    
    if not os.path.isdir(dir):
        raise ValueError("Directory does not exist.")
    
    allfiles = []
    dirpath = os.path.abspath(dir)
    
    for f in os.listdir(dirpath):
        fpath = os.path.join(dirpath, f)
        if os.path.isfile(fpath):
            allfiles.append({'fpath': fpath,    # directory + file name + extension
                             'dir': dirpath,                        # directory
                             'fname': f.split('.')[0],              # file name
                             'fext': os.path.splitext(fpath)[1]})   # extension
    return allfiles


def remove_nonsense_lines(line, min=4):
    counter = 0
    for c in line:
        if c in string.printable:
            counter += 1
        if counter >= min:
            return line
    return False

def read_and_store_pdfs_files(allfiles, index_name):
    
    i = 1
    n = len(allfiles)
    for f in allfiles:
        
        doc = {}
        
        if 'pdf' in f.get('fext'):
            
            eof = subprocess.check_output(['tail', '-1', f.get('fpath')])
            if b'%%EOF\n' != eof and b'%%EOF\r\n' != eof:
                print('Skipping ({}/{}): {}. Missing %%EOF file.'.format(i, n, f.get('fname')))
                continue
            
            print('Processing file ({}/{}): {}'.format(i, n, f.get('fname')))
            
            tpath = os.path.join(f.get('dir'), 'txt')
            if not os.path.exists(tpath):
                os.makedirs(tpath)
            
            fname = f.get('fname').replace(" ", "_")
            
            with open(f.get('fpath'), 'rb') as pdffile,  \
                codecs.open(os.path.join(tpath, fname + '.txt'), 'w', encoding="utf-8") as ftext:
                
                clean_text = ''
                text = textract.process(f.get('fpath'), encoding='utf-8')
                
                text = text.decode("utf-8")
                text = ''.join(list(filter(lambda x: x in set(string.printable), text)))
                text = text.split('\n')
                
                for line in text:
                    if not line and clean_text[-2:] != '\n\n':
                        clean_text += '\n'
                    else:
                        clean_line = remove_nonsense_lines(str(line), 8)
                        if clean_line:
                            clean_text += clean_line + '\n'
                
                ftext.write(clean_text)
                encoded = base64.b64encode(bytes(clean_text, 'utf-8'))
                
                pdf = PdfFileReader(pdffile, strict=False)
                info = pdf.getDocumentInfo()
                
                doc = {
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
                
                es_store_record(es, index_name, 'docs', doc)
        i += 1

if __name__ == '__main__':
    
    es = es_connect('localhost', 9200)
    
    if es is not None:
        
        index_name = 'gutenberg'
        '''
        if es.indices.exists(index_name):
            print("deleting {} index...".format(index_name))
            res = es.indices.delete(index = index_name)
            print(" response: {}".format(res))
        '''
        es_create_index(es, index_name)
        
        #dir = './files'
        dir = './data/gutenberg'
        allfiles = get_files_in_dir(dir)
        read_and_store_pdfs_files(allfiles, index_name)
        
        '''
        to_search = 'If Donald Trump becomes'
        search_object = {'query': 
                            {'match':
                                {'content': to_search}
                            }
                        }
        search_object = {'query': 
                            {'match_phrase':
                                {'content': to_search}
                            }
                        }
        result = es_search(es, 'repository', json.dumps(search_object))
        
        if result.get('hits') is not None and result['hits'].get('hits') is not None:
            print(result['hits']['hits'])
        else:
            print({})
        '''
    print('Exiting...')
    sys.exit(0)




