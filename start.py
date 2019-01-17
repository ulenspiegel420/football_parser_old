import chempionat_ru_parser as parser
import multiprocessing as mp
from database_components import Database
from common_functions import logging_error
from datetime import datetime

def parse_url(url):
    print(f"Season {url['year']}")
    db_name = 'football_stats_'+url['year']
    db = Database(db_name)

    db.sql_db.create_db()
    db.add_tables_to_db()

    parsing_tree = parser.Node(parser.ParsingTypes.root, data={'id': None, 'year': url['year'], 'url': url['url']})

    start_parsing = datetime.now()
    parser.parse_tournaments(parsing_tree)
    tournaments = parsing_tree.get_nodes_by_key(parser.ParsingTypes.tournament)

    parser.parse_teams(tournaments)
    teams = parsing_tree.get_nodes_by_key(parser.ParsingTypes.team)

    parser.parse_players(teams)
    players = parsing_tree.get_nodes_by_key(parser.ParsingTypes.player)

    db.add_rows_to_db(db.tables['tournaments'], tournaments)
    db.add_rows_to_db(db.tables['teams'], teams)
    db.add_rows_to_db(db.tables['players'], players)

    parser.parse_matches(tournaments)
    matches = parsing_tree.get_nodes_by_key(parser.ParsingTypes.match)

    db.add_rows_to_db(db.tables['matches'], matches)

    elapsed_time = datetime.now() - start_parsing

    parsed_items = parsing_tree.count()
    parsed_tournaments = parsing_tree.count_by_key(parser.ParsingTypes.tournament)
    parsed_teams = parsing_tree.count_by_key(parser.ParsingTypes.team)
    parsed_players = parsing_tree.count_by_key(parser.ParsingTypes.player)
    parsed_matches = parsing_tree.count_by_key(parser.ParsingTypes.match)

    print(f"\nParsed season {url['year']} for {elapsed_time.total_seconds()}sec.\n"
          f"Parsed {parsed_items} items.\n"
          f"    {parsed_tournaments} tournament(s)\n"
          f"    {parsed_teams} team(s)\n"
          f"    {parsed_players} player(s)\n"
          f"    {parsed_matches} match(es)\n"
          f"Inserted {db.sql_db.added_rows} rows.\n"
          f"Missed {len(db.sql_db.missing_added_rows)} rows.\n")

    if db.sql_db.missing_added_rows:
        try:
            with open("logs/missed_rows_" + url['year'] + ".log", 'a') as handle:
                rows = [str(row) for row in db.sql_db.missing_added_rows]
                for row in rows:
                    error = '\n'+row
                    handle.write(error.encode('utf-8', 'ignore').decode())
        except UnicodeEncodeError as e:
            raise Exception('Ошиабка записи невставленных строк'+',' + e.object+','+e.encoding)


def parsing(url):
    try:
        processes = []

        # Getting roots as seasons
        urls =  parser.get_urls_for_parsing(url)
        season_for_parsed = {'2014', '2015', '2016', '2017', '2018'}

        # for url in urls:
        #     # if url['year'] in season_for_parsed:
        #     processes.append(mp.Process(target=parse_url, args=(url,)))
        #
        # for process in processes:
        #     process.start()
        #
        # for process in processes:
        #     process.join()
        #
        for url in urls:
            if url['year'] == '2002':
                parse_url(url)

    except Exception as e:
        logging_error(str(e))
        return


if __name__ == '__main__':
    parsing("https://www.championat.com/stat/football/tournaments/2/domestic/")
    exit_code = ''
    print('Введите [exit] для выхода')
    while exit_code != 'exit':
        exit_code = input()
