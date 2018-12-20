from gevent import monkey
monkey.patch_all()
import gevent
from time import time
from elasticsearch import Elasticsearch
from elasticsearch.client.ingest import IngestClient
import utils
from control import logger, decfun


@decfun
class ES():
    
    def __init__(self, host='127.0.0.1', port=9200):

        self.es = None
        self.host = host
        self.port = port

    def is_connected(self):
        return True if self.es and self.es.ping() else False

    def connect(self):
        self.es = Elasticsearch([{'host': self.host, 'port': self.port}])
        if self.is_connected():
            msg = 'Connected to ElasticSearch on'
            logger.info('{msg} {host}:{port}'.format(msg=msg, host=self.host,
                                                      port=self.port))
        else:
            msg = 'Error. Failed to connect to Elasticsearch on'
            logger.error('{msg} {host}:{port}'.format(msg=msg, host=self.host,
                                                       port=self.port))


    def secure_delete_index(self, index_name):
        
        if not self.is_connected():
            logger.error('Error. Not connected to Elasticsearch')
            return

        if type(index_name) is not str:
            logger.error('Error. Index name must be a str')
            return

        msg = "Do you want to delete the index '{}'?".format(index_name)
        if self.es.indices.exists(index_name):
            if utils.query_yes_no(msg, False):
                res = self.es.indices.delete(index=index_name)
                logger.info("The index {} was deleted successfully".
                             format(index_name))
        return True


    def create_index(self, index_name, mapping=None):
        
        if not self.is_connected():
            logger.error('Error. Not connected to Elasticsearch')
            return
        
        if type(index_name) is not str:
            logger.error('Error. Index name must be a str')
            return
        
        if mapping and not isinstance(mapping, dict):
            logger.error('Error. Mapping must be a dictionary')
            return

        try:
            if not self.es.indices.exists(index_name):
                # Ignore 400 means to ignore "Index Already Exist" error.
                res = self.es.indices.create(index=index_name,
                                             body=mapping,
                                             ignore=[400, 404])
                logger.info(("Index '{}' was created successfully").
                            format(index_name))
            return True
        
        except Exception as ex:
            logger.error("Error creating the index '{}'.Error: {}".
                         format(index_name, str(ex)))
            return
    
    
    def store_record(self, index_name, doc_name, content):
        
        if not self.is_connected():
            logger.error('Error. Not connected to Elasticsearch')
            return
        
        if type(index_name) is not str:
            logger.error('Error. Index name must be a str')
            return
        
        if type(doc_name) is not str:
            logger.error('Error. Missing document name to store in Elasticsearch')
            return
        
        if not isinstance(content, dict):
            logger.error('Error. Missing content to store in Elasticsearch')
            return
        
        try:
            t1 = time()
            logger.debug("Gevent (before es_obj.index): '{}'".format(gevent.getcurrent().name))
            res = self.es.index(index=index_name, doc_type=doc_name, body=content)
            logger.debug("Gevent (after es_obj.index: '{}' - {}".format(gevent.getcurrent().name, time() - t1))
            return res
            
        except Exception as ex:
            logger.error('Error. Something went wrong storing the data')
            return
    
    
    def search(self, index_name, content):
        
        if not self.is_connected():
            logger.error('Error. Not connected to Elasticsearch')
            return
        
        if type(index_name) is not str:
            logger.error('Error. Index name must be a str')
            return

        if type(content) is not str:
            loggin.error('Error. Content must be a dictionary')
            return
        
        return self.es.search(index=index_name, body=content)
    
