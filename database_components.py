from mysql_operations import Db
from chempionat_ru_parser import ParsingTypes


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

    def __make_create_query(self):
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

        # except Exception as e:
        #     logging_error('Ошибка создания запроса на создание таблицы. Таблица: ' + self.name)
        #     logging_error(e)

    def __make_insert_query(self):
        fields_str = ','.join(map(lambda field: field.name, self.schema.fields))
        tpl = ','.join(['%s ' for i in range(len(self.schema.fields))])
        sql = f'INSERT INTO {self.name} ({fields_str}) VALUES (' + tpl + ');'
        return sql
        # except Exception as e:
        #     logging_error('Ошибка создания запроса на вставку. Таблица: ' + self.name)
        #     logging_error(e)

    def __make_select_id_query(self):
        fields_str = ' and '.join(map(lambda x: x.name+'=%s', [f for f in self.schema.fields if f.name != 'id']))
        sql = f'SELECT id FROM {self.name} WHERE {fields_str};'
        return sql


class Database:
    def __init__(self, db_name):
        self.sql_db = Db("config/mysql_cfg.ini", db_name)
        self.tables: dict = {}

        # season_fields = [
        #     Field('id', 'int', ['pk', 'auto']),
        #     Field('year', 'varchar(10)', ['unique', 'notnull']),
        #     Field('url', 'varchar(100)', [])
        # ]
        # self.__add_table(Table('seasons', Schema(season_fields)))

        tournament_fields = [
            Field('id', 'int', ['pk', 'auto']),
            # Field('year_id', 'int', ['fk', 'unique', 'notnull']),
            Field('name', 'varchar(100)', ['unique', 'notnull']),
            Field('country', 'varchar(50)', ['unique', 'notnull']),
            Field('start_date', 'datetime', ['unique', 'notnull']),
            Field('end_date', 'datetime', ['unique', 'notnull']),
            Field('url', 'varchar(150)', [])
        ]
        # season_fk = ForeignKey(tournament_fields[1], 'tournament_year_fk', 'seasons')
        self.__add_table(Table('tournaments', Schema(tournament_fields, [])))

        team_fields = [
            Field('id', 'int', ['pk', 'auto']),
            Field('tournament_id', 'int', ['fk', 'unique', 'notnull']),
            Field('name', 'varchar(100)', ['unique', 'notnull']),
            Field('city', 'varchar(100)', ['unique', 'notnull']),
            Field('url', 'varchar(150)', [])
        ]
        tournament_fk = ForeignKey(team_fields[1], 'team_tournament_fk', 'tournaments')
        self.__add_table(Table('teams', Schema(team_fields, [tournament_fk])))

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
        self.__add_table(Table('players', Schema(player_fields, [team_fk])))

        match_fields = [
            Field('id', 'int', ['pk', 'auto']),
            Field('home_team_id', 'int', ['fk', 'unique', 'notnull']),
            Field('guest_team_id', 'int', ['fk', 'unique', 'notnull']),
            Field('group_name', 'varchar(50)', []),
            Field('tour', 'varchar(10)', []),
            Field('match_date', 'varchar(20)', ['unique', 'notnull']),
            Field('home_score', 'varchar(10)', []),
            Field('guest_score', 'varchar(10)', []),
            Field('home_penalty_score', 'varchar(10)', []),
            Field('guest_penalty_score', 'varchar(10)', []),
            Field('is_extra_time', 'boolean', []),
        ]
        home_team_fk = ForeignKey(match_fields[1], 'home_team_fk', 'teams')
        guest_team_fk = ForeignKey(match_fields[2], 'guest_team_fk', 'teams')
        self.__add_table(Table('matches', Schema(match_fields, [home_team_fk, guest_team_fk])))

    def __add_table(self, table: Table):
        self.tables[table.name] = table

    def add_tables_to_db(self):
        [self.sql_db.create_table(table.sql_create) for table in self.tables.values()]

    def add_rows_to_db(self, table: Table, nodes: list):
        rows = list(map(lambda x: tuple(x.data.values()), nodes))
        self.sql_db.insert_rows_async(table.sql_insert, rows)
        self.__update_id_for_rows(table, nodes)

    def __update_id_for_rows(self, table, nodes: list):
        for node in nodes:
            values = list(map(lambda x: x, [v for f, v in node.data.items() if f != 'id']))
            id = self.sql_db.get_id(table.sql_select_id, tuple(values))
            node.data['id'] = id
            if node.key == ParsingTypes.tournament:
                for child in node.get_children():
                    child.data['tournament_id'] = id
            elif node.key == ParsingTypes.team:
                for child in node.get_children():
                    child.data['team_id'] = id