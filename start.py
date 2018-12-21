from chempionat_ru_parser import *
from mysql_operations import Db
import parsing_functions
import bs4


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

def add_teams_to_db(team_nodes, db):
    for node in team_nodes:
        team_row = (node.name, node.city, node.parent.db_id)
        if not db.team_exist(node.name, node.parent.db_id):
            node.db_id = db.add_team(*team_row)
        else:
            db.passed_teams += 1
            node.db_id = db.get_team_id(*team_row)


def add_players_to_db(player_nodes, db):
    for node in player_nodes:
        player_row = (node.name,
                      node.role,
                      node.nationality,
                      node.birth,
                      node.growth,
                      node.weight,
                      node.parent.db_id)
        if not db.player_exist(*player_row):
            db.add_player(*player_row)
        else:
            db.passed_players += 1


def add_matches_to_db(match_nodes, db):
    for node in match_nodes:
        if node.home == node.parent.name:
            home_id = node.parent.db_id
            team_row = (node.guest, node.parent.parent.db_id)
            guest_id = db.get_team_id_by_name(*team_row)
        else:
            guest_id = node.parent.db_id
            team_row = (node.home, node.parent.parent.db_id)
            home_id = db.get_team_id_by_name(*team_row)
        match_row = (node.tour,
                     node.match_date,
                     home_id,
                     guest_id,
                     node.home_result,
                     node.guest_result,
                     node.penalty_home,
                     node.penalty_guest)
        if not db.match_exist(*match_row):
            db.add_play(*match_row)
        else:
            db.passed_matches += 1


def parse_season(url):
    try:
        request = parsing_functions.get_request(url)
        if request is None:
            return

        soup = bs4.BeautifulSoup(request.text, 'html.parser')
        content = soup.find('div', 'js-tournament-header-year').find_all('option')
        if content is None:
            return
        season_links = []
        for item in content:
            season_url = "https://www.championat.com" + item['data-href']
            season = item.get_text().lstrip().rstrip().split("/")[0]
            season_links.append({'url': season_url, 'season': season})
        return season_links
    except Exception as e:
        print(e)
        return


def parsing(url):
    site = "https://www.championat.com"
    db_name_prefix = 'football_stats_'

    try:
        for season_link in parse_season(url):
            print('***********************************************************************************************')
            print('Parsing season: ', season_link['season'], '\n')

            db_name = db_name_prefix + season_link['season']
            database = Database(db_name)

# Creating schema

            tournament_fields = [
                Field('id', 'int', ['pk', 'auto']),
                Field('name', 'varchar(100)', ['unique', 'notnull']),
                Field('country', 'varchar(50)', ['unique', 'notnull']),
                Field('start_date', 'datetime', ['unique', 'notnull']),
                Field('end_date', 'datetime', ['unique', 'notnull'])
            ]
            database.add_table(Table('tournaments', Schema(tournament_fields)))

            team_fields = [
                Field('id', 'int', ['pk', 'auto']),
                Field('tournament_id', 'int', ['fk', 'unique', 'notnull']),
                Field('name', 'varchar(100)', ['unique', 'notnull']),
                Field('city', 'varchar(100)', ['unique', 'notnull'])
            ]
            tournament_fk = ForeignKey(team_fields[1], 'team_tournament_fk', 'tournaments')
            database.add_table(Table('teams', Schema(team_fields, [tournament_fk])))



# Parsing and adding data

            tree = ParsingTree(url)

            tree.parse_tournaments(season_link['url'])
            tournaments = tree.get_nodes_by_key(tree.ParsingTypes.tournament)
            database.add_rows_to_db(database.tables['tournaments'], tournaments)

            tree.parse_teams(tournaments)
            teams = tree.get_nodes_by_key(tree.ParsingTypes.team)
            database.add_rows_to_db(database.tables['teams'], teams)



            tree.parse_players(teams)

        #     print('Parsed tournaments: ', tree.parsed_tournaments, ' Added: ', db.added_tournaments,
        #           ' Passed: ', db.passed_tournaments)
        #     print('Parsed teams: ', tree.parsed_teams, ' Added: ', db.added_teams, ' Passed: ', db.passed_teams)
        #     print('Parsed players: ', tree.parsed_players, ' Added: ', db.added_players, ' Passed: ', db.passed_players)
        #     print('Parsed matches: ', tree.parsed_matches, ' Added: ', db.added_matches, ' Passed: ', db.passed_matches)
        #
        # with open("logs/missed_rows.log", 'w') as handle:
        #     for row in db.missing_added_rows:
        #         handle.write(row)

    except Exception as e:
        print(e)
        return


if __name__ == '__main__':
    parsing("https://www.championat.com/stat/football/tournaments/2/domestic/")
    exit = ''
    print('Введите [exit] для выхода')
    while exit != 'exit':
        exit = input()