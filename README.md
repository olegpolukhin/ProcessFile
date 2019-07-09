# Система сбора данных и выгрузка в таблицу

Данные собираются из уже загруженных в базу данных access_log. Процесс включает в себя экспорт данных в новую базу данных, на основе которых и создается финальный отчет.

## Системные требования

* Python 2.7

## Конфигурация `settings.py`
```python
CLICKHOUSE = {
    'host': 'localhost',
	'port': '9000',
    'db_export': '',
    'bin': ''
}

BASE_TABLE = ''

FILE_LOG = 'synclogs.txt'

WATCHER = {
    'timedelta': 86400*2
}
```

## Скрипт процесса экспорта и импорта `process.py`

Скрипт проверяет базу данных `db_export` на наличие ещё не обработанных данных не старее чем `timedelta`. 
Если такие данные будут найдены, то они будут экспортированы в таблицу `BASE_TABLE`.
Обработанные данные записываются в файл `FILE_LOG`. Если данные в файле совпадают с таблицей, повторно запись не произойдёт.


## Схема данных для импорта

Скрипт `process.py` создаёт таблицу на ноде шарда и в неё импортирует данные из таблиц `log_%`:
Ниже код таблицы которая будет создана.

```sql
CREATE TABLE {host}.{BASE_TABLE} (
        date Date,
        type_id UInt16,
		item_id UInt32,
        tube_id UInt32,
        country String,
        num_views AggregateFunction(count)) 
        ENGINE = AggregatingMergeTree(date, (date, type_id, tube_id, country, item_id), 8192)
```

## Запуск скрипта

Для запуска из терминала или в кроне использовать команду `sh process.sh`.
