from bs4 import BeautifulSoup as bs
import re
import parsing_functions
from datetime import datetime
from enum import Enum
import common_functions as common


class Node:
    def __init__(self, key, data=None):
        self.key = key
        self.children = []
        self.parents = []
        self.data = data
        self.url: str = None

    def set_parent(self, node):
        self.parents.append(node)

    def set_child(self, node):
        self.children.append(node)

    def get_parents(self):
        return self.parents

    def get_children(self):
        return self.children

# class Match(Node):
#     def __init__(self, tour, match_date, home, guest, home_result, guest_result, penalty_home, penalty_guest):
#         super().__init__(None)
#         self.db_tour = tour
#         self.db_date = match_date
#         self.home: Team = home
#         self.guest: Team = guest
#         self.db_home_result = home_result
#         self.db_guest_result = guest_result
#         self.db_home_penalty = penalty_home
#         self.db_guest_penalty = penalty_guest
#         self.parent = None
#         self.tournament = None
#         self.fkeys = {'fk_home_match': 'teams', 'fk_guest_match': 'teams'}
#         self.not_nulls = ['db_tour', 'db_date']
#         self.ukeys = ['db_tour', 'db_date', 'db_home_id', 'db_guest_id']
#
#
# class Player(Node):
#     def __init__(self, name, role, nationality, birth, growth, weight):
#         super().__init__(None)
#         self.db_name = name
#         self.db_role = role
#         self.db_nationality = nationality
#         self.db_birth = birth
#         self.db_growth = growth
#         self.db_weight = weight
#         self.fkeys = {'fk_team_player': 'teams'}
#         self.not_nulls = ['db_name']
#         self.ukeys = ['db_team_id', 'db_name', 'db_birth']
#
#
# class Team(Node):
#     def __init__(self, name, city):
#         super().__init__(None)
#         self.db_name = name
#         self.db_city = city
#         self.db_tournament_id = 0
#         self.match_children = []
#         self.parent = None
#         self.players_url = None
#         self.matches_url = None
#         self.ukeys = ['db_id', 'db_name', 'db_city']
#         self.not_nulls = ['db_name', 'db_city']
#         self.fkeys = {'fk_tournament_team': 'tournaments'}
#
#     def add_player_node(self, player_node):
#         self.add_node(player_node)
#
#     def add_match_node(self, match_node):
#         self.match_children.append(match_node)
#
#
# class Tournament(Node):
#     def __init__(self, name, country, start_date, end_date):
#         super().__init__(None)
#         self.parent = None
#         self.db_startdate = start_date
#         self.db_enddate = end_date
#         self.db_name = name
#         self.db_country = country
#         self.teams_url = None
#         self.not_nulls = ['db_enddate', 'db_startdate', 'db_name', 'db_country']
#         self.ukeys = ['db_enddate', 'db_startdate', 'db_name', 'db_country']
#
#     def add_team_node(self, team_node):
#         self.add_node(team_node)
#
#
# class SeasonNode(Node):
#     def __init__(self, season, season_url):
#         super().__init__(None)
#         self.season = season
#         self.season_url = season_url
#         self.parent = None
#         self.tournaments = []
#
#     def add_tournament_node(self, tournament_node):
#         self.add_node(tournament_node)
#         self.tournaments.append(tournament_node)
#
#     def get_tournaments(self):
#         return self.children


class ParsingTree:
    class ParsingTypes(Enum):
        root = 0
        tournament = 1
        team = 2
        player = 3
        match = 4
        season = 5

    def __init__(self, root=None):
        self.root = root if root is not None else Node(self.ParsingTypes.root, None)
        self.parsed_items = 0
        self.common_url = "https://www.championat.com/football"
        self.site = "https://www.championat.com"
        self.teams_url: list = []
        self.parsed_tournaments = 0
        self.parsed_teams = 0
        self.parsed_players = 0
        self.parsed_matches = 0
        self.seasons = []
        self.tournaments = []
        self.teams = []
        self.players = []
        self.matches = []

    def parse_tournaments(self, season):
        try:
            request = parsing_functions.get_request(season.data['url'])
            soup = bs(request.text, 'html.parser')
            content = soup.find('div', 'mc-sport-tournament-list').find_all('div', 'mc-sport-tournament-list__item')
            for item in content:
                country = item.find('div', 'item__title').get_text().lstrip().rstrip()
                html_links = item.find_all(attrs={"data-type": "tournament"})
                for html_link in html_links:
                    t_name = html_link['data-title'].lstrip().rstrip()
                    html_link.find('span', 'separator').extract()
                    t_dates_html = html_link.findNext('div', 'item__dates _dates').findAll('span')
                    t_start_date = datetime.strptime(t_dates_html[0].get_text().lstrip().rstrip(), "%d.%m.%Y")
                    t_end_date = datetime.strptime(t_dates_html[1].get_text().lstrip().rstrip(), "%d.%m.%Y")

                    data = {'id': None,
                            'season_id': season.data['id'],
                            'name': t_name,
                            'country': country,
                            'start_date': t_start_date,
                            'end_date': t_end_date,
                            'url': self.site + html_link['href']}

                    tournament = Node(self.ParsingTypes.tournament, data)
                    tournament.set_parent(season)
                    season.set_child(tournament)

                    self.tournaments.append(tournament)
                    self.parsed_items += 1
        except Exception as e:
            print('Ошибка парсинга турниров')
            print(e)

    def parse_teams(self):
        try:
            for tournament in self.tournaments:
                url = tournament.data['url'] + 'teams'
                content = parsing_functions.get_contents(url,'a', 'teams-item__link')
                if content is not None:
                    for item in content:
                        name = item.find('div', 'teams-item__name').get_text().lstrip().rstrip()
                        city = item.find('div', 'teams-item__country').get_text().lstrip().rstrip()

                        data = {'id': None,
                                'tournament_id': tournament.data['id'],
                                'name': name,
                                'city': city}

                        team = Node(self.ParsingTypes.team, data)
                        team.url = self.site + item['href'].replace('result/', 'players/')
                        team.set_parent(tournament)
                        tournament.set_child(team)

                        self.teams.append(team)
                        self.parsed_items += 1
                else:
                    common.logging_warning(f'Ошибка получения контента команды по url: {url}')
        except Exception as e:
            print('Ошибка парсинга команд')
            print(e)

    def parse_players(self):
        try:
            for team in self.teams:
                content = parsing_functions.get_content(team.url, 'div', 'js-tournament-filter-content')
                if content is not None:
                    player_rows = content.tbody.findAll('tr')
                    for item in player_rows:
                        name = item.find(attrs={'class': 'table-item__name'}).text.lstrip().rstrip()
                        role = item.find(attrs={'data-label': 'Амплуа'}).text.lstrip().rstrip()
                        birth = item.find(attrs={'data-label': 'ДР'}).text.lstrip().rstrip()
                        growth = item.find(attrs={'data-label': 'Рост'}).text.lstrip().rstrip()
                        weight = item.find(attrs={'data-label': 'Вес'}).text.lstrip().rstrip()
                        nationality = '/'.join([country['title'] for country in item.find_all(class_='_country_flag')])

                        data = {'id': None,
                                'team_id': team.data['id'],
                                'name': name,
                                'nationality': nationality,
                                'role': role,
                                'birth': birth,
                                'growth': growth,
                                'weight': weight}

                        player = Node(self.ParsingTypes.player, data)
                        player.set_parent(team)
                        team.set_child(player)

                        self.players.append(player)
                        self.parsed_items += 1
                else:
                    common.logging_warning(f"Ошибка получения контента игроков команды "
                                           f"{team.data['name']} по url: {team.url}")
        except Exception as e:
            raise Exception(f'Ошибка парсинга игроков. '+str(e))

    def parse_matches(self):
        try:
            for tournament in self.tournaments:
                url = tournament.data['url'] + 'calendar'
                content = parsing_functions.get_content(url, 'table', 'stat-results__table')
                if content is not None:
                    rows = content.tbody.find_all('tr')

                    for html_row in rows:
                        group_html_row = html_row.find('td', 'stat-results__group')
                        group = group_html_row.text.lstrip().rstrip() if group_html_row is not None else None

                        tour = html_row.find('td', 'stat-results__tour-num').text.lstrip().rstrip()
                        match_date = html_row.find('td', 'stat-results__date-time').text.lstrip().rstrip()
                        match_date = re.sub(r'\s+', ' ', match_date)

                        teams = html_row.find_all('span', 'stat-results__title-team')
                        home_team_elem = teams[0].a
                        guest_team_elem = teams[1].a
                        if home_team_elem or guest_team_elem is not None:
                            home_team_id = self.search_node(home_team_elem.text.lstrip().rstrip(), 'name', tournament).data['id']
                            guest_team_id = self.search_node(guest_team_elem.text.lstrip().rstrip(), 'name', tournament).data['id']
                        else:
                            common.logging_warning(
                                f"Ошибка парсинга матчей турнира {tournament.data['name']} по url: {url}")
                            continue

                        is_extra_time = False
                        penalty_home, penalty_guest, home_result, guest_result = None, None, None, None
                        main_result = html_row.find('span', 'stat-results__count-main').text.lstrip().rstrip().split(':')
                        home_result, guest_result = main_result[0], main_result[1]

                        extra_html = html_row.find('span', 'stat-results__count-ext')
                        if extra_html is not None:
                            extra_result = extra_html.text.lstrip().rstrip()
                            if extra_result == 'ДВ':
                                is_extra_time = True
                            if re.match(r"\d:\d", extra_result) is not None:
                                extra_result = extra_result.split(':')
                                penalty_home = extra_result[0]
                                penalty_guest = extra_result[1]

                        data = {'id': None,
                                'home_team_id': home_team_id,
                                'guest_team_id': guest_team_id,
                                'group_name': group,
                                'tour': tour,
                                'match_date': match_date,
                                'home_score': home_result,
                                'guest_score': guest_result,
                                'home_penalty_score': penalty_home,
                                'guest_penalty_score': penalty_guest,
                                'is_extra_time': is_extra_time}

                        match = Node(self.ParsingTypes.match, data)
                        match.set_parent(tournament)
                        tournament.set_child(match)

                        self.matches.append(match)
                        self.parsed_items += 1
                else:
                    common.logging_warning(f"Ошибка парсинга матчей турнира {tournament.data['name']} по url: {url}")
        except Exception as e:
            raise Exception('Ошибка парсинга матчей по url: ' + str(e))

    @staticmethod
    def parse_seasons(url):
        try:
            content = parsing_functions.get_content(url, 'div', 'js-tournament-header-year')
            if content is None:
                common.logging_warning('Ошибка парсинга сезонов. Ошибка получения контента')
                return

            items = content.find_all('option')
            seasons = []
            for item in items:
                season_url = "https://www.championat.com" + item['data-href']
                year = item.get_text().lstrip().rstrip().split("/")[0]
                data = {'id': None,
                        'year': year,
                        'url': season_url}
                season = Node(ParsingTree.ParsingTypes.season, data)
                seasons.append(season)
            return seasons
        except Exception as e:
            raise Exception('Ошибка парсинга сезонов. ' + str(e))

    def get_nodes_by_key(self, key: ParsingTypes,  node=None):
        # results = []
        # for item in self.parsed_items:
        #     if item.key == key:
        #         results.append(item)
        # return results

        if node is None:
            node = self.root
        if len(node.children) == 0:
            return
        result = []
        for child in node.children:
            if child.key == key:
                result.append(child)
            items = self.get_nodes_by_key(key, child)
            if items is not None:
                    result.extend(items)
        return result

    def search_node(self, value: str, field: str, node=None):
        if node is None:
            node = self.root

        if node.data[field] == value:
            return node

        for child in node.children:
            if child.data[field] == value:
                return child
            else:
                self.search_node(value, field, child)
