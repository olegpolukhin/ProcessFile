import os,errno
from log import logger as logger

CLICKHOUSE = {
    'port': '9000',
    'host': 'localhost',
    'db_export': '',
    'bin': ''
}

BASE_TABLE = ''

FILE_LOG = 'synclogs.txt'

WATCHER = {
    'timedelta': -86400*2
}

def check_file():
    """Create sync file"""
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        q=os.open(FILE_LOG, flags)
        os.close(q)
    except Exception:
        return False
    else:
        return True

def remove_file():
    try:
        os.remove(FILE_LOG)
    except Exception:
        logger.error('Can\'t delete file: %s', FILE_LOG)


def process_file(text):
    with open(FILE_LOG, 'a') as file:
        file.writelines(text + '\n')
        os.utime(FILE_LOG, None)


def get_data_file():
    file = open(FILE_LOG, 'r') 
    return file.read().splitlines() 
