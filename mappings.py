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
                    "role": {"type": "keyword"},
                    "name": {
                        "type": "nested",
                        "properties": {
                            "first": { "type": "text" },
                            "last": { "type": "text" }
                        }
                    },
                    "email": { "type": "keyword" },
                    "phone": { "type": "keyword" },
                    "comment": {"type": "text" },
                    "is_active": { "type": "boolean" },
                    "created": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
                    "last_access": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
                    "ip_addr": { "type": "ip" }
                }
            }
        }
    },
    "files": {
        "settings" : {
            "number_of_shards" : 1,
            "number_of_replicas" : 1
        },
        "mappings": {
            "_doc": {
                "dynamic": "strict",
                "properties": {
                    "id": { "type": "keyword" },
                    "info": {
                        "type": "nested",
                        "properties": {
                            "path": { "type": "keyword" },
                            "directory": { "type": "keyword" },
                            "filename": { "type": "keyword" },
                            "extension": { "type": "keyword" },
                            "hash_content": { "type": "keyword" },
                            "pages": { "type": "short" }
                        }
                    },
                    "title": { "type": "text", "analyzer": "english" },
                    "content": { "type": "text", "analyzer": "english" },
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