mappings = {
    "users": {
        "settings" : {
            "number_of_shards" : 1,
            "number_of_replicas" : 1
        },
        "mappings": {
            "_doc": {
                "dynamic": "strict",
                "properties": {
                    "id": {"type": "text"},
                    "name": {
                        "type": "nested",
                        "properties": {
                            "first": { "type": "text" },
                            "last": { "type": "text" }
                        }
                    },
                    "email": { "type": "keyword" },
                    "phone": { "type": "keyword" },
                    "ip_addr": { "type": "ip" }
                }
            }
        }
    },
    "files": {
        "settings": {
            "number_of_shards" : 1,
            "number_of_replicas" : 1,
            "analysis": {
                "tokenizer": {
                    "ngram_tokenizer": {
                        "type": "ngram",
                        "min_gram": 2,
                        "max_gram": 30,
                        "token_chars": ["letter", "digit"]
                    }
                },
                "filter": {
                    "my_synonym_filter": {
                        "type": "synonym",
                        "synonyms": [
                            "english,british",
                            "usa,united states of america,us"
                        ]
                    }
                },
                "char_filter": {
                    "my_html_filter": {
                        "type": "html_strip"
                    }
                },
                "analyzer": {
                    "my_analyzer": {
                        "tokenizer": "ngram_tokenizer",
                        "char_filter": ["my_html_filter"],
                        "filter": [
                            "lowercase",
                            "asciifolding",
                            "my_synonym_filter"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "_doc": {
                "_all": {
                    "store": "true"
                },
                "dynamic": "strict",
                "properties": {
                    "id": { "type": "keyword" },
                    "meta": {
                        "type": "nested",
                        "properties": {
                            "dir_root": { "type": "keyword" },
                            "folder_file": { "type": "keyword" },
                            "folder_img": { "type": "keyword" },
                            "filename": { "type": "keyword" },
                            "extension": { "type": "keyword" },
                            "content_sha512_hex": { "type": "keyword" },
                            'title': { "type": "keyword" },
                            'creator': { "type": "keyword" },
                            'producer': { "type": "keyword" },
                            'tragged': { "type": "keyword" },
                            'user_properties': { "type": "keyword" },
                            'suspects': { "type": "keyword" },
                            'from': { "type": "keyword" },
                            'javascript': { "type": "keyword" },
                            'pages': { "type": "short" },
                            'encripted': { "type": "keyword" },
                            'page_size': { "type": "keyword" },
                            'page_rot': { "type": "keyword" },
                            'file_size': { "type": "keyword" },
                            'optimized': { "type": "keyword" },
                            'pdf_version': { "type": "keyword" },
                            'creation_date': { "type": "keyword" },
                            'author': { "type": "keyword" }
                        }
                    },
                    "title": { "type": "text", "analyzer": "english" },
                    "content": { "type": "text", "analyzer": "english" },
                    "content_base64": { "type": "binary" },
                    "summary": { "type": "text", "analyzer": "english" },
                    "user_id": { "type": "keyword" },
                    "created": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
                    "tags": { "type": "text" }
                }
            }
        }
    },
    "comments": {
        "settings" : {
            "number_of_shards" : 1,
            "number_of_replicas" : 1
        },
        "mappings": {
            "comments": {
                "dynamic": "strict",
                "properties": {
                    "title": { "type": "text" },
                    "comment": { "type": "text" },
                    "stars": { "type": "short" },
                    "author": { "type": "text" },
                    "date": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
                    "parent_id": { "type": "keyword" }
                }
            }
        }
    }
}

'''
mapping_relationships = {
    "settings" : {
        "number_of_shards" : 1,
        "number_of_replicas" : 1
    },
    "mappings": {
        "_doc": {
            "dynamic": "strict",
            "properties": {
                "my_join_field": {
                    "type": "join",
                    "relations": {
                        "question": ["answer", "comment"],
                        "answer": "vote"
                    }
                }
            }
        }
    }
}
'''
