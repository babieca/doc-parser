import os

dir_path = os.path.dirname(os.path.realpath(__file__))
log_path = os.path.join(dir_path, 'logs.log')

config = {
    
    'app': {
        'dir_root': '../gutenberg/',
        'ignore_same_docs': True,
        'freq_min': 5,
        'logfile': log_path
    },
    
    'elasticsearch': {
        'host': '127.0.0.1',
        'port': 9200, 
    }
}