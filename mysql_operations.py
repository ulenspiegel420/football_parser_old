from configparser import ConfigParser
from mysql.connector import MySQLConnection, Error, errorcode
import multiprocessing as mp
from datetime import datetime
from functools import reduce


class Db:
    class Connection(MySQLConnection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def close(self):
            super().close()

    def __init__(self, path_to_mysql_config):
        self.path_to_mysql_config = path_to_mysql_config
        self.missing_added_rows = []
        self.sql_params = self._read_db_config()
        self.added_rows = 0
        self.db_name = None

    def _add_to_missing(self, sql_error, row):
        self.missing_added_rows.append({str(sql_error.errno): sql_error.msg, 'data': row})

    def _read_db_config(self, section='mysql'):
        """ Read database configuration file and return a dictionary object"""
        parser = ConfigParser()
        parser.read(self.path_to_mysql_config)
        db = {}
        if parser.has_section(section):
            items = parser.items(section)
            for item in items:
                db[item[0]] = item[1]
        else:
            raise Exception('{0} not found in the {1} file'.format(section, self.path_to_mysql_config))
        return db

    def _get_connection(self):
        """ Connect to MySQL database """
        try:
            connection = MySQLConnection(**self._read_db_config())
            if connection.is_connected():
                return connection
        except Error as e:
            if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif e.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(e)

    def create_db(self):
        query = 'CREATE DATABASE ' + self.db_name
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            cursor = connection.cursor()
            cursor.execute(query)
        except Error as e:
            print(e)
        except Exception as e:
            print('Ошибка создания базы данных ', self.db_name)
            print(e)
        finally:
            connection.close()

    def create_table(self, sql):
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            connection.database = self.db_name
            cursor = connection.cursor()
            cursor.execute(sql)
        except Error as e:
            print(e)
        except Exception as e:
            print('Ошибка создания таблицы ', sql)
            print(e)
        finally:
            connection.close()

    def _insert_row(self, rows, sql):
        connection = self.Connection(**self.sql_params)
        connection.database = self.db_name
        success_rows = 0
        missed_rows = []
        cursor = connection.cursor(buffered=True)
        for row in rows:
            try:
                cursor.execute(sql, row)
                connection.commit()
                success_rows += 1
            except Error as e:
                missed_rows.append({'error': e.errno, 'data': row})
                continue
        cursor.close()
        connection.close()
        return {'added_count': success_rows, 'missed_rows': missed_rows}

    def insert_multiproc_rows(self, sql: str, rows: list):
        pool = mp.Pool(processes=4)
        added = 0
        missed = []
        results = []
        sub_rows = []
        start = datetime.now()
        n = 100
        if len(rows) >= n:
            while len(rows) != 0:
                if len(rows) >= n:
                    sub_rows.append([rows.pop() for _ in range(n)])
                else:
                    sub_rows.append([rows.pop() for _ in range(len(rows))])

            results = [pool.apply_async(self._insert_row, args=(separated_rows, sql)) for separated_rows in sub_rows]
            pool.close()
            pool.join()
        else:
            sub_rows = [rows.pop() for _ in range(len(rows))]
            results = [pool.apply_async(self._insert_row, args=(sub_rows, sql))]
            pool.close()
            pool.join()
        added = reduce((lambda x, y: x+y), [result.get()['added_count'] for result in results])
        [missed.extend(result.get()['missed_rows']) for result in results]
        self.added_rows += added
        self.missing_added_rows.extend(missed)
        elapsed = datetime.now() - start
        print(f"Inserted {added} rows to db for {elapsed.total_seconds()}sec. Missed {len(missed)}\n")

    def insert_rows(self, sql: str, rows: list):
        added = 0
        missed = []
        connection = self.Connection(**self.sql_params)
        connection.database = self.db_name
        cursor = connection.cursor(buffered=True)
        start = datetime.now()
        for row in rows:
            try:
                cursor.execute(sql, row)
                connection.commit()
                added += 1
            except Error as e:
                missed.append({'error': e.errno, 'data': row})
                continue
        cursor.close()
        connection.close()
        elapsed = datetime.now() - start
        self.added_rows += added
        self.missing_added_rows.extend(missed)
        print(f"Inserted {added} rows to db for {elapsed.total_seconds()}sec. Missed {len(missed)}\n")

    def get_id(self, sql, field):
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            connection.database = self.db_name
            cursor = connection.cursor()
            cursor.execute(sql, field)
            result = cursor.fetchone()
        except Error as e:
            print(e)
        except Exception as e:
            print('Ошибка получения ид.')
            print(e)
        finally:
            connection.close()
            return result[0] if result else result
