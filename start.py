from chempionat_ru_parser import *
import multiprocessing as mp
from database_components import Database, Table, ForeignKey, Schema, Field
from common_functions import logging_error


def parse_season(season: Node, name):
    print(f"Season {season.data['year']}")
    db = Database(name)

    start_parsing = datetime.now()

    tree = ParsingTree(season)
    tree.seasons.append(season)
    tree.parsed_items += 1

    tree.parse_tournaments(season)
    tree.parse_teams()
    tree.parse_players()

    db.add_rows_to_db(db.tables['seasons'], tree.seasons)
    db.add_rows_to_db(db.tables['tournaments'], tree.tournaments)
    db.add_rows_to_db(db.tables['teams'], tree.teams)
    db.add_rows_to_db(db.tables['players'], tree.players)

    tree.parse_matches()
    db.add_rows_to_db(db.tables['matches'], tree.matches)

    elapsed_time = datetime.now() - start_parsing

    print(f"\nParsed season {season.data['year']} for {elapsed_time.total_seconds()}sec.\n"
          f"Parsed {tree.parsed_items} items.\n"
          f"Inserted {db.sql_db.added_rows} rows.\n"
          f"Missed {len(db.sql_db.missing_added_rows)} rows.\n")

    if len(db.sql_db.missing_added_rows) != 0:
        with open("logs/missed_rows.log", 'a') as handle:
            for row in db.sql_db.missing_added_rows:
                str = row['error']+':'+row['data']+'\n'
                handle.write(str)


def parsing(url):
    db_name = 'football_stats'
    db = Database(db_name)
    db.sql_db.create_db()
    db.add_tables_to_db()

    try:
        processes = []
        seasons = ParsingTree.parse_seasons(url)

        for season in seasons:
            processes.append(mp.Process(target=parse_season, args=(season, db_name)))

        for process in processes:
            process.start()

        for process in processes:
            process.join()

        # for season in seasons:
        #     database.sql_db.added_rows = 0
        #     database.sql_db.missing_added_rows = []
        #     parse_season(season, database)

    except Exception as e:
        logging_error(str(e))
        return


if __name__ == '__main__':
    parsing("https://www.championat.com/stat/football/tournaments/2/domestic/")
    exit_code = ''
    print('Введите [exit] для выхода')
    while exit_code != 'exit':
        exit_code = input()
