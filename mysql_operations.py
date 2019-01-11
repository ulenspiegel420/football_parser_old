from configparser import ConfigParser
from mysql.connector import MySQLConnection, Error, errorcode
import multiprocessing as mp
from functools import reduce


class Db:
    def __init__(self, path_to_mysql_config, db_name):
        self.path_to_mysql_config = path_to_mysql_config
        self.missing_added_rows = []
        self.sql_params = self._read_db_config()
        self.added_rows = 0
        self.db_name = db_name

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
            connection = MySQLConnection(**self.sql_params)
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
        connection, cursor, result = None, None, None
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            cursor.execute(query)
        except Error as e:
            print(e)
        except Exception as e:
            print('Ошибка создания базы данных ', self.db_name)
            print(e)
        finally:
            cursor.close()
            connection.close()

    def create_table(self, sql):
        connection, cursor, result = None, None, None
        try:
            connection = self._get_connection()
            connection.database = self.db_name
            cursor = connection.cursor()
            cursor.execute(sql)
        except Error as e:
            print(e)
        except Exception as e:
            print('Ошибка создания таблицы ', sql)
            print(e)
        finally:
            cursor.close()
            connection.close()

    def _insert_row(self, rows, sql):
        connection, cursor, result = None, None, None
        success_rows = 0
        missed_rows = []
        connection = self._get_connection()
        connection.database = self.db_name
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

    def insert_rows_async(self, sql: str, rows: list):
        pool = mp.Pool(processes=4)

        sub_rows = []
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

        self.added_rows += reduce((lambda x, y: x+y), [result.get()['added_count'] for result in results])
        ([self.missing_added_rows.extend(result.get()['missed_rows']) for result in results])

    def insert_rows(self, sql: str, rows: list):
        connection = self._get_connection()
        connection.database = self.db_name
        cursor = connection.cursor(buffered=True)
        for row in rows:
            try:
                cursor.execute(sql, row)
                connection.commit()
                self.added_rows += 1
            except Error as e:
                self.missing_added_rows.append({'error': e.errno, 'data': row})
                continue
        cursor.close()
        connection.close()

    def get_id(self, sql, field):
        connection, cursor, result = None, None, None
        try:
            connection = self._get_connection()
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
            cursor.close()
            connection.close()
            return result[0] if result else result
