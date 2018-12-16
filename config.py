config_es = {
    'host': '127.0.0.1',
    'port': 9200,
    'index': 'gutenberg',
    'documents': 'pdfs',
    'path': './data/gutenberg',
    'ignore_same_docs': True,
    'frequency': 5,
    'mapping': {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "pdfs": {
                # dynamic: strict --> the “doc” object will throw an exception
                # if an unknown field is encountered
                "dynamic": "strict",
                "properties": {
                    "id": {"type": "text"},
                    "info": {
                        "type": "nested",
                        "properties": {
                            "path": {"type": "keyword"},
                            "directory": {"type": "keyword"},
                            "filename": {"type": "keyword"},
                            "extension": {"type": "keyword"},
                            "hash_content": {"type": "keyword"}
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
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "author": {
                        "type": "nested",
                        "properties": {
                            "id": {"type": "text"},
                            "name": {"type": "text"},
                            "email": {"type": "keyword"},
                            "comment": {"type": "text"},
                            "date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"}
                        }
                    },
                    "created": {
                        "type": "nested",
                        "properties": {
                            "id": {"type": "text"},
                            "name": {"type": "text"},
                            "comment": {"type": "text"},
                            "date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"}
                        }
                    },
                    "comments": {
                        "type": "nested",
                        "properties": {
                            "name": {"type": "text"},
                            "comment": {"type": "text"},
                            "age": {"type": "short"},
                            "stars": {"type": "short"},
                            "date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"}
                        }
                    },
                    "tags": {"type": "text"}
                }
            }
        }
    }
}