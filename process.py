import sys
import os
from datetime import date, datetime

import re
import datetime

from log import logger as logger
import settings
import clickhouse

def check_table():
    sql = """EXISTS TABLE {db}.{name}"""\
            .format(db=settings.CLICKHOUSE['db_export'], name=settings.BASE_TABLE)
    res = clickhouse.query(sql)
    if (res['returncode'] == 0):
        if str(res['out'].splitlines()).strip('[]') == "'1'":
            return True
        else:
            return False        
    else:
        logger.error('There is an error: %s', repr(res['err']))
        sys.exit(-1)

def drop_table():
    sql = "DROP TABLE IF EXISTS {db}.{name}"\
        .format(db=settings.CLICKHOUSE['db_export'], name=settings.BASE_TABLE)
    res = clickhouse.query(sql)

    if (res['returncode'] != 0):
        logger.error('There is an error: %s', repr(res['err']))
        sys.exit(-1)

def create_table():
    sql = """CREATE TABLE {db}.{name} (
        date Date,
        type_id UInt16,
		item_id UInt32,
        tube_id UInt32,
        country String,
        num_views AggregateFunction(count)) 
        ENGINE = AggregatingMergeTree(date, (date, type_id, tube_id, country, item_id), 8192)"""\
            .format(db=settings.CLICKHOUSE['db_export'], name=settings.BASE_TABLE)
    res = clickhouse.query(sql)
    if (res['returncode'] != 0 and res['returncode'] != 57):
        logger.error('There is an error: %s', repr(res['err']))
        sys.exit(-1)


def get_tables():
    """Get available table names from clickhouse that contains log data"""
    sql = "select name from system.tables where database='{db}' and name like 'log_%_%_%'"\
        .format(db=settings.CLICKHOUSE['db_export'])
    res = clickhouse.query(sql)
    if (res['returncode'] == 0):
        tables = res['out'].splitlines()
        tables.sort()
        # Do not take the most recent table, because it can be still in use
        return tables[:-1]
    else:
        logger.error('There is an error: %s', repr(res['err']))
        sys.exit(-1)


def filter_tables_time_predicate(table):
    """Predicate to test if the table as recent as required"""
    m = re.search(
        'log_([0-9]{4,4})([0-9]{2,3})([0-9]{2,3})_([0-9]{2,3})([0-9]{2,3})([0-9]{2,3})', table)
    if(m is None):
        return False

    if(settings.WATCHER['timedelta'] < 0):
        return True

    d = datetime.datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                          int(m.group(4)), int(m.group(5)), int(m.group(6)))
    delta = datetime.datetime.now()-d
    return delta.total_seconds() < settings.WATCHER['timedelta']


logger.info("Start export")

if check_table() != True:
    logger.info("The table does not exist yet. Create a '%s' table.", settings.BASE_TABLE)
    settings.remove_file()
    create_table()
else:
    logger.info("The table already exists, get the tables")

settings.check_file()

tables = get_tables()
if len(tables) == 0:
    logger.warn("Tables null")
    sys.exit(-1)

# print tables

tables = filter(filter_tables_time_predicate, tables)

dataFile = settings.get_data_file()
if (len(dataFile) == 0):
    drop_table()
    create_table()

# print tables
for table in tables:
    if table != '' and any(table in t for t in dataFile) != True:
        logger.info("Sync '%s' and export it's data", table)

        sql = """INSERT INTO {db}.{base_table} (date, type_id, item_id, tube_id, country, num_views) 
        SELECT `date` AS date, type_id, item_id, site_id AS tube_id, country, countState() AS num_views FROM {db}.{table} 
        WHERE (type_id = 5) AND (date >= subtractDays(today(), 10)) AND ((response_code >= 200) AND (response_code <= 299)) AND (agent_robot = '') AND (site_id < 'A') AND (item_id > 0) AND (src = 'hostedtube') AND match(site_id, '^[0-9]+$') 
        GROUP BY date, type_id, item_id, site_id, country
        """.format(db=settings.CLICKHOUSE['db_export'], table=table, base_table=settings.BASE_TABLE)
        err = clickhouse.query(sql)
        if err['returncode'] != 0:
            logger.error("There is an exporting error: %s", err)
            continue
        else:
            settings.process_file(table)

    else:
        logger.info("The table '%s' already exists", table)