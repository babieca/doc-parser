#!/usr/bin/env python

import os
import sys
import string
import codecs
import json
from datetime import datetime
from elasticsearch import Elasticsearch
import textract

def es_connect(host='localhost', port=9200):
    _es = Elasticsearch([{'host': host, 'port': port}])
    if _es.ping():
        print('Connect to ElasticSearch on {}:{}'.format(host, port))
    else:
        print('It could not connect to ElasticSearch!')
    return _es


def es_create_pdf_index(es_object, index_name='repository'):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "pdfs": {
                "dynamic": "strict",
                "properties": {
                    "title": {
                        "type": "text"
                    },
                    "content": {
                        "type": "text"
                    },
                }
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
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


def read_pdf_files(allfiles):
    
    for f in allfiles:
        
        doc = {}
        
        if 'pdf' in f.get('fext'):
            
            tpath = os.path.join(f.get('dir'), 'txt')
            if not os.path.exists(tpath):
                os.makedirs(tpath)
            
            fname = f.get('fname').replace(" ", "_")
            with open(f.get('fpath'), 'rb') as pdf_file,   \
                codecs.open(os.path.join(tpath, fname + '.txt'), 'w', "utf-8") as ftext:
                
                clean_text = ''
                text = textract.process(f.get('fpath'))
                text = text.decode("utf-8")
                text = text.split('\n')
                for line in text:
                    if not line and clean_text[-2:] != '\n\n':
                        clean_text += '\n'
                    else:
                        clean_line = remove_nonsense_lines(str(line), 8)
                        if clean_line:
                            clean_text += clean_line + '\n'
                
                ftext.write(clean_text)
                
                doc = {'title': f.get('fname'), 'content': clean_text}
                
                es_store_record(es, 'repository', 'pdfs', doc)


if __name__ == '__main__':
    
    es = es_connect('localhost', 9200)
    
    if es is not None:
        es_create_pdf_index(es, 'repository')
        
        dir = './files'
        allfiles = get_files_in_dir(dir)
        read_pdf_files(allfiles)
        
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
        
    print('Exiting...')
    sys.exit(0)




