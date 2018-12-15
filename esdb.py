from gevent import monkey
monkey.patch_all()
import gevent
import logging
from time import time
from elasticsearch import Elasticsearch
from elasticsearch.client.ingest import IngestClient
import utils


class ES():
    
    def __init__(self, host='127.0.0.1', port=9200, idx=None, mapping=None):

        self.es = None
        self.host = host
        self.port = port
        self.index = idx
        self.mapping = mapping


    def is_connected(self):
        return True if self.es and self.es.ping() else False


    def get_index(self):
        if self.index:
            return self.index
        else:
            logging.info('Error. Index not defined')
            return None


    def set_index(self, index_name):
        if index_name and type(index_name) is str:
            self.index = index_name
        else:
            self.index = None
            logging.info('Error setting the index name')


    def get_mapping(self):
        if self.mapping:
            return self.mapping
        else:
            logging.info('Info. Mapping not defined')
            return None


    def set_mapping(self, mp=None):
        if mp and isinstance(mp, dict):
            self.mapping = mp
        else:
            logging.info('Error setting the mapping')


    def connect(self):
        es = Elasticsearch([{'host': self.host, 'port': self.port}])
        if es.ping():
            self.es = es
            msg = 'Connected to ElasticSearch on'
            logging.info('{msg} {host}:{port}'.format(msg=msg, host=self.host,
                                                      port=self.port))
        else:
            msg = 'Error. Failed to connect to Elasticsearch on'
            logging.error('{msg} {host}:{port}'.format(msg=msg, host=self.host,
                                                       port=self.port))

    
    def secure_delete_index(self, index_name=None):
        
        if index_name:
            self.set_index(index_name)
        else:
            index_name = self.get_index()

        logging.debug('')
        if self.es and self.is_connected() and index_name:
            msg = "Do you want to delete the index '{}'?".format(index_name)
            if self.es.indices.exists(index_name):
                if utils.query_yes_no(msg):
                    res = self.es.indices.delete(index=index_name)
                    logging.info("The index {} was deleted successfully".format(
                        index_name))
                else:
                    logging.info("The index {} was not deleted".format(self.index))


    def create_index(self, index_name=None, mapping=None):
        
        if index_name:
            self.set_index(index_name)
        else:
            index_name = self.get_index()
        
        if mapping:
            self.set_mapping(mapping)
        else:
            mapping = self.get_mapping()

        if self.es and self.is_connected() and index_name:
            try:
                if not self.es.indices.exists(index_name):
                    # Ignore 400 means to ignore "Index Already Exist" error.
                    res = self.es.indices.create(index=self.get_index(),
                                                 body=mapping,
                                                 ignore=[400, 404])
                    logging.info('Index {} was created successfully. Response: {}'.format(index_name, res))
                else:
                    logging.info('Index {} was not created, it already exists.'.format(index_name))
            
            except Exception as ex:
                logging.info('Something went wrong creating the index {}. Error: {}'.format(index_name, str(ex)))
    
    
    def store_record(self, index_name=None, doc_name=None, content=None):
        
        if index_name:
            self.set_index(index_name)
        else:
            index_name = self.get_index()
        
        if not doc_name or type(doc_name) is not str:
            loggin.error('Error. Missing document name to store in Elasticsearch')
            return
        
        if not content or type(content) is not str:
            loggin.error('Error. Missing content to store in Elasticsearch')
            return

        if self.es and self.is_connected() and index_name:
            try:
                t1 = time()
                logging.debug('Gevent (before es_obj.index): {}'.format(gevent.getcurrent().name))
                outcome = self.es.index(index=index_name, doc_type=doc_type, body=content)
                logging.debug('Gevent (after es_obj.index: {} - {}'.format(gevent.getcurrent().name, time() - t1))
                
            except Exception as ex:
                logging.error('Error. Something went wrong storing the data. {}'.format(str(ex)))
    
    
    def search(self, index_name=None, content=None):
        
        if index_name:
            self.set_index(index_name)
        else:
            index_name = self.get_index()


        if not content or type(content) is not str:
            loggin.error('Error. Missing content to search in Elasticsearch')
            return
        
        self.es.search(index=index_name, body=content)
    
