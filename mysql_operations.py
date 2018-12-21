from configparser import ConfigParser
from mysql.connector import MySQLConnection, Error, errorcode


class Db:
    class Connection(MySQLConnection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def close(self):
            super().close()

    def _add_to_missing(self, sql_error, row):
        self.missing_added_rows.append({str(sql_error.errno): sql_error.msg, 'data': row})

    def __init__(self, path_to_mysql_config):
        self.path_to_mysql_config = path_to_mysql_config
        self.missing_added_rows = []
        self.sql_params = self._read_db_config()
        self.added_tournaments = 0
        self.added_matches = 0
        self.added_players = 0
        self.added_teams = 0
        self.passed_tournaments = 0
        self.passed_teams = 0
        self.passed_players = 0
        self.passed_matches = 0
        self.db_name = None

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

    def add_play(self, tour, date, id_home, id_guest, home_result, guest_result, penalty_home, penalty_guest):
        query = 'INSERT INTO plays(tour, date, id_home, id_guest, home_result, guest_result, penalty_home,'\
                'penalty_guest) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) '
        row = (tour, date, id_home, id_guest, home_result, guest_result, penalty_home, penalty_guest)
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            cursor = connection.cursor()
            cursor.execute(query, row)
            result = cursor.lastrowid
            connection.commit()
            self.added_matches += 1
        except Error as e:
            self._add_to_missing(e, row)
        except Exception as e:
            print('Ошибка добавления матча')
            print(e)
        finally:
            connection.close()
            return result

    def add_tournament(self, name, country, start_date, end_date):
        query = 'INSERT INTO tournaments(name, country, start_date, end_date) VALUES(%s,%s,%s,%s)'
        row = (name, country, start_date, end_date)
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            cursor = connection.cursor()
            cursor.execute(query, row)
            result = cursor.lastrowid
            connection.commit()
            self.added_tournaments += 1
        except Error as e:
            self._add_to_missing(e, row)
        except Exception as e:
            print('Ошибка добавления турнира')
            print(e)
        finally:
            connection.close()
            return result

    def add_team(self, name, city, tournament_id):
        query = "INSERT INTO teams(name, city, tournamentId) VALUES(%s,%s,%s)"
        row = (name, city, tournament_id)
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            cursor = connection.cursor()
            cursor.execute(query, row)
            result = cursor.lastrowid
            connection.commit()
            self.added_teams += 1
        except Error as e:
            self._add_to_missing(e, row)
        except Exception as e:
            print('Ошибка добавления команды')
            print(e)
        finally:
            connection.close()
            return result

    def add_player(self, name, role, nationality, birth, growth, weight, team_id):
        query = "INSERT INTO players(name,role,nationality,birth,growth,weight,team_id) VALUES(%s,%s,%s,%s,%s,%s,%s)"
        row = (name, role, nationality, birth, growth, weight, team_id)
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            cursor = connection.cursor()
            cursor.execute(query, row)
            result = cursor.lastrowid
            connection.commit()
            self.added_players += 1
        except Error as e:
            self._add_to_missing(e, row)
        except Exception as e:
            print('Ошибка добавления игрока')
            print(e)
        finally:
            connection.close()
            return result

    def get_tournament_id(self, name, country, start_date, end_date):
        """Получение ID команды по ее имени из БД"""
        query = "SELECT id FROM tournaments WHERE name=%s and country=%s and start_date=%s and end_date=%s"
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            cursor = connection.cursor()
            cursor.execute(query, (name, country, start_date, end_date))
            result = cursor.fetchone()
        except Error as e:
            print(e)
        except Exception as e:
            print('Ошибка получения ид турнира')
            print(e)
        finally:
            connection.close()
            return result[0]

    def get_team_id(self, name, city, tournament_id):
        query = "SELECT id FROM teams WHERE name=%s and city=%s and tournamentid=%s"
        row = (name, city, tournament_id)
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            cursor = connection.cursor()
            cursor.execute(query, row)
            result = cursor.fetchone()
        except Error as e:
            print(e)
        except Exception as e:
            print('Ошибка получения ид команды')
            print(e)
        finally:
            connection.close()
            return result[0] if result else result

    def get_team_id_by_name(self, name, tournament_id):
        query = "SELECT id FROM teams WHERE name=%s and tournamentid=%s"
        row = (name, tournament_id)
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            cursor = connection.cursor()
            cursor.execute(query, row)
            result = cursor.fetchone()
        except Error as e:
            print(e)
        except Exception as e:
            print('Ошибка получения ид команды через имя')
            print(e)
        finally:
            connection.close()
            return result[0] if result else result

    def match_exist(self, tour, date, id_home, id_guest, home_result, guest_result, penalty_home, penalty_guest):
        query = "SELECT id FROM plays WHERE tour=%s and date=%s and id_home=%s and id_guest=%s"
        row = (tour, date, id_home, id_guest)
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            cursor = connection.cursor()
            cursor.execute(query, row)
            result = cursor.fetchone()
        except Error as e:
            print(e)
        except Exception as e:
            print('Ошибка проверки существования матча')
            print(e)
        finally:
            connection.close()
            return True if result else False

    def player_exist(self, name, role, nationality, birth, growth, weight, team_id):
        query = "SELECT id FROM players WHERE name=%s and birth=%s and team_id=%s"
        row = (name, birth, team_id)
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            cursor = connection.cursor()
            cursor.execute(query, row)
            result = cursor.fetchone()
        except Error as e:
            print(e)
        except Exception as e:
            print('Ошибка проверки существоания игрока')
            print(e)
        finally:
            connection.close()
            return True if result else False

    def team_exist(self, name, tournament_id):
        query = "SELECT * FROM teams WHERE name=%s and tournamentid=%s"
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            cursor = connection.cursor()
            cursor.execute(query, (name, tournament_id))
            result = cursor.fetchone()
        except Error as e:
            print(e)
        except Exception as e:
            print('Ошибка проверки сущестования команды')
            print(e)
        finally:
            connection.close()
            return True if result else False

    def tournament_exist(self, name, country, start_date, end_date):
        query = "SELECT id FROM tournaments WHERE name=%s and country=%s and start_date=%s and end_date=%s"
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            cursor = connection.cursor()
            cursor.execute(query, (name, country, start_date, end_date))
            result = cursor.fetchone()
        except Error as e:
            print(e)
        except Exception as e:
            print('Ошибка проверки существования турнира')
            print(e)
        finally:
            connection.close()
            return True if result else False

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

    def insert_rows(self, sql, rows):
        connection, result = None, None
        try:
            connection = self.Connection(**self.sql_params)
            connection.database = self.db_name
            cursor = connection.cursor()
            cursor.executemany(sql, rows)
            connection.commit()
        except Error as e:
            print(e)
        except Exception as e:
            print('Ошибка вставки данных ', sql)
            print(e)
        finally:
            connection.close()

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


# def insert_rows(query, rows):
#     adding_start = time.perf_counter()
#     print('Adding '+str(len(rows))+ ' rows in database...')
#     try:
#         connection = _get_connection()
#         cursor = connection.cursor()
#         cursor.executemany(querye, rows)
#         connection.commit()
#     except Error as error:
#         print(error)
#     finally:
#         cursor.close()
#         connection.close()
#
#         now = time.perf_counter()
#         adding_elapsed = now - adding_start
#         print('Elapsed time: ', '{:0.3f}'.format(adding_elapsed))

# def add_tournaments(tournaments):
#     query = 'INSERT INTO tournaments(name, country, start_date, end_date) VALUES(%s,%s,%s,%s)'
#     try:
#         connection = _get_connection()
#         cursor = connection.cursor()
#         cursor.executemany(query, tournaments)
#         connection.commit()
#     except Error as e:
#         if e.errno!=1062:
#             print("Error code:", e.errno)       # error number
#             print("SQLSTATE value:", e.sqlstate) # SQLSTATE value
#             print("Error message:", e.msg)       # error message
#     finally:
#         if cursor.rowcount == -1: result = False
#         else: result = True
#         _close_connection(connection)
#
#     return result
#

#
# def insert_row(query, row):
#     try:
#         connection = _get_connection()
#         cursor = connection.cursor()
#         cursor.execute(query, row)
#         lastrowid = cursor.lastrowid
#         connection.commit()
#     except Error as e:
#         print(e)
#     finally:
#         cursor.close()
#         connection.close()
#         return lastrowid
#
# def query_all():
#     try:
#         connection = _get_connection()
#         cursor = connection.cursor()
#         cursor.execute("SELECT * FROM teams")
#         rows = cursor.fetchall()
#
#         for row in rows: print(row)
#     except Error as e:
#         print(e)
#     finally:
#         _close_connection(connection)
#
# def get_query_result(query,row):
#     try:
#         connection = _get_connection()
#         cursor = connection.cursor()
#         cursor.execute(query,row)
#         result = cursor.fetchone()
#     except Error as e:
#         print(e)
#     finally:
#         _close_connection(connection)
#         return result
#
# def get_team_names():
#     """Получение множества названий команд из БД"""
#     try:
#         connection = _get_connection()
#         cursor = connection.cursor()
#         cursor.execute("SELECT name FROM teams")
#         resullt = cursor.fetchall()
#     except Error as e:
#         print(e)
#     finally:
#         _close_connection(connection)
#
#         team_names = set()
#         for team_name in resullt:
#             team_names.add(team_name[0])
#
#         return team_names
#
# def get_all_plays():
#     """Получение всех игр из БД"""
#     try:
#         connection = _get_connection()
#         cursor = connection.cursor()
#         cursor.execute("SELECT tour, play_date, id_left_team, id_right_team, left_result, right_result, liga FROM plays")
#         resullt = cursor.fetchall()
#     except Error as e:
#         print(e)
#     finally:
#         _close_connection(connection)
#         return resullt
#
# def get_all_tournaments():
#     connection = _get_connection()
#     cursor = connection.cursor()
#     cursor.execute("SELECT name, country FROM tournaments")
#     resullt = cursor.fetchall()
#     _close_connection(connection)
#     return resullt
#
# def get_all_teams():
#     """Получение всех команд из БД"""
#     try:
#         connection = _get_connection()
#         cursor = connection.cursor()
#         cursor.execute("SELECT name, country FROM teams")
#         result = cursor.fetchall()
#     except Error as e:
#         print(e)
#     finally:
#         _close_connection(connection)
#         return result
#
#
# def row_exist(query, row):
#     try:
#         connection = _get_connection()
#         cursor = connection.cursor()
#         cursor.execute(query,row)
#         result = cursor.fetchone()
#     except Error as e:
#         print(e)
#     finally:
#         _close_connection(connection)
#         if not result: return False
#         return True