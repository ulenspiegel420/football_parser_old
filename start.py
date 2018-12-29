from chempionat_ru_parser import *
from mysql_operations import Db


class Field:
    def __init__(self, name, sql_type, options: list):
        self.name = name
        self.type = sql_type
        self.options = options


class PrimaryKey:
    try:
        def __init__(self, field: Field):
            self.key_str = f'PRIMARY KEY ({field.name})'
    except Exception as e:
        print('Ошибка создания первичного ключа')
        print(e)


class ForeignKey:
    try:
        def __init__(self, field: Field, name: str, table: str):
            self.key_str = f'CONSTRAINT {name} FOREIGN KEY ({field.name}) REFERENCES {table}(id)'
    except Exception as e:
        print('Ошибка создания внешнего ключа')
        print(e)


class UniqueKey:
    try:
        def __init__(self, fields: list, name: str):
            fields_str = ','.join(field.name for field in fields)
            self.key_str = f'CONSTRAINT {name} UNIQUE ({fields_str})'
    except Exception as e:
        print('Ошибка создания уникального индекса')
        print(e)


class Schema:
    def __init__(self, fields: list, fkeys: list = None):
        self.fields: list = fields
        self.pk: PrimaryKey = self.__make_primary_key()
        self.fk = fkeys if fkeys is not None else []
        self.uk: UniqueKey = self. __make_unique_key()

    def __make_primary_key(self):
        for field in self.fields:
            if 'pk' in field.options:
                return PrimaryKey(field)

    def __make_unique_key(self):
        fields = []
        for field in self.fields:
            if 'unique' in field.options:
                fields.append(field)
        return UniqueKey(fields, 'unique_idx')

    def add_foreign_key(self, key: ForeignKey):
        self.fk.append(key)


class Table:
    def __init__(self, name: str, schema: Schema):
        self.name = name
        self.schema = schema
        self.sql_create = self.__make_create_query()
        self.sql_insert = self.__make_insert_query()
        self.sql_select_id = self.__make_select_id_query()

    def __get_sql_type(self, python_type: type):
        sql_type = 'varchar(100)'
        if python_type == datetime:
            sql_type = 'datetime'
        elif python_type == int:
            sql_type = 'int'
        return sql_type

    def __make_create_query(self):
        try:
            query_items = []
            for field in self.schema.fields:
                sql_field = field.name + ' ' + field.type
                if 'notnull' in field.options:
                    sql_field += ' NOT NULL'
                if 'auto' in field.options:
                    sql_field += ' AUTO_INCREMENT'
                query_items.append(sql_field)

            if self.schema.pk is not None:
                query_items.append(self.schema.pk.key_str)
            if len(self.schema.fk) != 0:
                query_items.extend(key.key_str for key in self.schema.fk)
            if self.schema.uk is not None:
                query_items.append(self.schema.uk.key_str)

            query_elements_str = ','.join(query_items)

            sql = f'CREATE TABLE {self.name} ({query_elements_str});'
            return sql
        except Exception as e:
            print('Ошибка создания запроса на создание таблицы. Таблица: ' + self.name)
            print(e)

    def __make_insert_query(self):
        try:
            fields_str = ','.join(map(lambda field: field.name, self.schema.fields))
            tpl = ','.join(['%s ' for i in range(len(self.schema.fields))])
            sql = f'INSERT INTO {self.name} ({fields_str}) VALUES ('+tpl+');'
            return sql
        except Exception as e:
            print('Ошибка создания запроса на вставку. Таблица: ' + self.name)
            print(e)

    def __make_select_id_query(self):
        fields_str = ' and '.join(map(lambda x: x.name+'=%s', [f for f in self.schema.fields if f.name != 'id']))
        sql = f'SELECT id FROM {self.name} WHERE {fields_str};'
        return sql


class Database:
    def __init__(self, name):
        self.sql_db = Db("config/mysql_cfg.ini")
        self.sql_db.db_name = name
        self.tables: dict = {}
        self.sql_db.create_db()

    def add_table(self, table: Table):
        self.tables[table.name] = table
        self.sql_db.create_table(table.sql_create)

    def add_rows_to_db(self, table: Table, nodes: list):
        rows = list(map(lambda x: tuple(x.data.values()), nodes))
        self.sql_db.insert_rows(table.sql_insert, rows)
        self.__update_id_for_rows(table, nodes)

    def __update_id_for_rows(self, table, nodes: list):
        for node in nodes:
            values = list(map(lambda x: x, [v for f, v in node.data.items() if f != 'id']))
            node.data['id'] = self.sql_db.get_id(table.sql_select_id, tuple(values))


def parsing(url):
    db_name = 'football_stats'
    database = Database(db_name)
    # Creating schemas
    year_fields = [
        Field('id', 'int', ['pk', 'auto']),
        Field('year', 'varchar(10)', ['unique', 'notnull']),
        Field('url', 'varchar(100)', [])
    ]
    database.add_table(Table('seasons', Schema(year_fields)))

    tournament_fields = [
        Field('id', 'int', ['pk', 'auto']),
        Field('year_id', 'int', ['fk', 'unique', 'notnull']),
        Field('name', 'varchar(100)', ['unique', 'notnull']),
        Field('country', 'varchar(50)', ['unique', 'notnull']),
        Field('start_date', 'datetime', ['unique', 'notnull']),
        Field('end_date', 'datetime', ['unique', 'notnull']),
        Field('url', 'varchar(100)', [])
    ]
    year_fk = ForeignKey(tournament_fields[1], 'tournament_year_fk', 'seasons')
    database.add_table(Table('tournaments', Schema(tournament_fields, [year_fk])))

    team_fields = [
        Field('id', 'int', ['pk', 'auto']),
        Field('tournament_id', 'int', ['fk', 'unique', 'notnull']),
        Field('name', 'varchar(100)', ['unique', 'notnull']),
        Field('city', 'varchar(100)', ['unique', 'notnull'])
    ]
    tournament_fk = ForeignKey(team_fields[1], 'team_tournament_fk', 'tournaments')
    database.add_table(Table('teams', Schema(team_fields, [tournament_fk])))

    player_fields = [
        Field('id', 'int', ['pk', 'auto']),
        Field('team_id', 'int', ['fk', 'unique', 'notnull']),
        Field('name', 'varchar(100)', ['unique', 'notnull']),
        Field('nationality', 'varchar(50)', []),
        Field('role', 'varchar(20)', []),
        Field('birth', 'varchar(15)', ['unique']),
        Field('growth', 'varchar(10)', []),
        Field('weight', 'varchar(10)', [])
    ]
    team_fk = ForeignKey(player_fields[1], 'player_team_fk', 'teams')
    database.add_table(Table('players', Schema(player_fields, [team_fk])))

    match_fields = [
        Field('id', 'int', ['pk', 'unique', 'auto']),
        Field('home_team_id', 'int', ['fk', 'unique', 'notnull']),
        Field('guest_team_id', 'int', ['fk', 'unique', 'notnull']),
        Field('group_name', 'varchar(50)', []),
        Field('tour', 'varchar(10)', ['notnull']),
        Field('match_date', 'varchar(20)', ['notnull']),
        Field('home_score', 'varchar(10)', []),
        Field('guest_score', 'varchar(10)', []),
        Field('home_penalty_score', 'varchar(10)', []),
        Field('guest_penalty_score', 'varchar(10)', []),
        Field('is_extra_time', 'boolean', []),
    ]
    home_team_fk = ForeignKey(match_fields[1], 'home_team_fk', 'teams')
    guest_team_fk = ForeignKey(match_fields[2], 'guest_team_fk', 'teams')
    database.add_table(Table('matches', Schema(match_fields, [home_team_fk, guest_team_fk])))

    try:
        seasons = ParsingTree.parse_seasons(url)
        print('Parsed seasons: ', len(seasons))
        database.add_rows_to_db(database.tables['seasons'], seasons)
        # Parsing and adding data
        for season in seasons:
            database.sql_db.added_rows = 0
            database.sql_db.missing_added_rows = []
            print('**************************************************************************')
            print('Parsing season: ', season.data['year'], '\n')
            tree = ParsingTree(url)
            tree.create_root()
            start_parsing = datetime.now()
            tree.parse_tournaments(season)
            elapsed_time = datetime.now()-start_parsing
            print('Parsed tournaments: ', tree.parsed_tournaments, ', for ', elapsed_time.total_seconds(), 'sec.')
            tournaments = tree.get_nodes_by_key(tree.ParsingTypes.tournament)
            database.add_rows_to_db(database.tables['tournaments'], tournaments)

            start_parsing = datetime.now()
            tree.parse_teams(tournaments)
            elapsed_time = datetime.now() - start_parsing
            print('Parsed teams: ', tree.parsed_teams, ', for ', elapsed_time.total_seconds(), 'sec.')
            teams = tree.get_nodes_by_key(tree.ParsingTypes.team)
            database.add_rows_to_db(database.tables['teams'], teams)

            start_parsing = datetime.now()
            tree.parse_players(teams)
            elapsed_time = datetime.now() - start_parsing
            print('Parsed players: ', tree.parsed_players, ' for ', elapsed_time.total_seconds(), 'sec.')
            players = tree.get_nodes_by_key(tree.ParsingTypes.player)
            if len(players) != 0:
                database.add_rows_to_db(database.tables['players'], players)

            start_parsing = datetime.now()
            tree.parse_matches(tournaments)
            elapsed_time = datetime.now() - start_parsing
            print('Parsed matches: ', tree.parsed_matches, ', for ', elapsed_time.total_seconds(), 'sec.')
            matches = tree.get_nodes_by_key(tree.ParsingTypes.match)
            database.add_rows_to_db(database.tables['matches'], matches)

            print('Added rows: ', database.sql_db.added_rows)
            print('Missed rows', len(database.sql_db.missing_added_rows))

            if len(database.sql_db.missing_added_rows) != 0:
                with open("logs/missed_rows.log", 'a') as handle:
                    for row in database.sql_db.missing_added_rows:
                        handle.write(','.join([str(field) for field in row['data']])+"\n")

    except Exception as e:
        print(e)
        return


if __name__ == '__main__':
    parsing("https://www.championat.com/stat/football/tournaments/2/domestic/")
    exit_code = ''
    print('Введите [exit] для выхода')
    while exit_code != 'exit':
        exit_code = input()
