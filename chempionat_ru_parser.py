import bs4
from bs4 import BeautifulSoup as bs
import re
import parsing_functions
from multiprocessing import Process
from datetime import datetime
from enum import Enum


class Node:
    def __init__(self, key, data=None):
        self.key = key
        self.children = []
        self.parents = []
        self.data = data
        self.url: str = None

    def add_node(self, node):
        self.children.append(node)

    def get_children(self):
        return self.children


class Match(Node):
    def __init__(self, tour, match_date, home, guest, home_result, guest_result, penalty_home, penalty_guest):
        super().__init__(None)
        self.db_tour = tour
        self.db_date = match_date
        self.home: Team = home
        self.guest: Team = guest
        self.db_home_result = home_result
        self.db_guest_result = guest_result
        self.db_home_penalty = penalty_home
        self.db_guest_penalty = penalty_guest
        self.parent = None
        self.tournament = None
        self.fkeys = {'fk_home_match': 'teams', 'fk_guest_match': 'teams'}
        self.not_nulls = ['db_tour', 'db_date']
        self.ukeys = ['db_tour', 'db_date', 'db_home_id', 'db_guest_id']


class Player(Node):
    def __init__(self, name, role, nationality, birth, growth, weight):
        super().__init__(None)
        self.db_name = name
        self.db_role = role
        self.db_nationality = nationality
        self.db_birth = birth
        self.db_growth = growth
        self.db_weight = weight
        self.fkeys = {'fk_team_player': 'teams'}
        self.not_nulls = ['db_name']
        self.ukeys = ['db_team_id', 'db_name', 'db_birth']


class Team(Node):
    def __init__(self, name, city):
        super().__init__(None)
        self.db_name = name
        self.db_city = city
        self.db_tournament_id = 0
        self.match_children = []
        self.parent = None
        self.players_url = None
        self.matches_url = None
        self.ukeys = ['db_id', 'db_name', 'db_city']
        self.not_nulls = ['db_name', 'db_city']
        self.fkeys = {'fk_tournament_team': 'tournaments'}

    def add_player_node(self, player_node):
        self.add_node(player_node)

    def add_match_node(self, match_node):
        self.match_children.append(match_node)


class Tournament(Node):
    def __init__(self, name, country, start_date, end_date):
        super().__init__(None)
        self.parent = None
        self.db_startdate = start_date
        self.db_enddate = end_date
        self.db_name = name
        self.db_country = country
        self.teams_url = None
        self.not_nulls = ['db_enddate', 'db_startdate', 'db_name', 'db_country']
        self.ukeys = ['db_enddate', 'db_startdate', 'db_name', 'db_country']

    def add_team_node(self, team_node):
        self.add_node(team_node)


class SeasonNode(Node):
    def __init__(self, season, season_url):
        super().__init__(None)
        self.season = season
        self.season_url = season_url
        self.parent = None
        self.tournaments = []

    def add_tournament_node(self, tournament_node):
        self.add_node(tournament_node)
        self.tournaments.append(tournament_node)

    def get_tournaments(self):
        return self.children


class ParsingTree:
    class ParsingTypes(Enum):
        root = 0
        tournament = 1
        team = 2
        player = 3
        match = 4

    def __init__(self, url):
        self.url = url
        self.root = Node(self.ParsingTypes.root)
        self.common_url = "https://www.championat.com/football"
        self.site = "https://www.championat.com"
        self.teams_url: list = []
        self.parsed_tournaments = 0
        self.parsed_teams = 0
        self.parsed_players = 0
        self.parsed_matches = 0

    def parse_tournaments(self, url):
        request = parsing_functions.get_request(url)
        if request is None:
            return
        try:
            soup = bs(request.text, 'html.parser')
            content = soup.find_all('div', 'mc-sport-tournament-list__item')
            if content is None:
                return
            for item in content:
                country = item.find('div', 'item__title').get_text().lstrip().rstrip()
                html_links = item.find_all(attrs={"data-type": "tournament"})
                for html_link in html_links:
                    # html_id = re.findall(r'/(\d+)', html_link['href'])[0]
                    # html_type = re.findall(r'_\w+', html_link['href'])[0]
                    t_name = html_link['data-title'].lstrip().rstrip()

                    html_link.find('span', 'separator').extract()

                    t_dates_html = html_link.findNext('div', 'item__dates _dates').findAll('span')
                    t_start_date = datetime.strptime(t_dates_html[0].get_text().lstrip().rstrip(), "%d.%m.%Y")
                    t_end_date = datetime.strptime(t_dates_html[1].get_text().lstrip().rstrip(), "%d.%m.%Y")
                    # tournament_url = html_type + "/" + html_id + "/tournir/info.html"
                    # teams_url = self.common_url + html_type + "/" + html_id + "/teams.html"
                    # plays_url = html_type + "/" + html_id + "/calendar.html"
                    # plays_group_url = html_type + "/" + html_id + "/calendar/group.html"
                    # plays_playoff_url = html_type + "/" + html_id + "/calendar/playoff.html"
                    # plays_preliminary_url = html_type + "/" + html_id + "/calendar/preliminary.html"

                    data = {'id': None,
                            'name': t_name,
                            'country': country,
                            'start_date': t_start_date,
                            'end_date': t_end_date}

                    node = Node(self.ParsingTypes.tournament, data)
                    node.url = self.site + html_link['href']
                    self.root.add_node(node)
                    node.parents.append(self.root)

                    self.parsed_tournaments += 1

        except Exception as e:
            print('Ошибка парсинга турниров')
            print(e)

    def parse_teams(self, nodes):
        for node in nodes:
            request = parsing_functions.get_request(node.url + 'teams')
            if request is None:
                return
            try:
                soup = bs(request.text, 'html.parser')
                content = soup.find_all('a', 'teams-item__link')
                for item in content:
                    name = item.find('div', 'teams-item__name').get_text().lstrip().rstrip()
                    city = item.find('div', 'teams-item__country').get_text().lstrip().rstrip()
                    data = {'id': None,
                            'tournament_id': node.data['id'],
                            'name': name,
                            'city': city}
                    team = Node(self.ParsingTypes.team, data)
                    team.url = self.site+item['href'].replace('result/', 'players/')
                    team.parents.append(node)

                    node.add_node(team)

                    self.parsed_teams += 1
            except Exception as e:
                print('Ошибка парсинга команд')
                print(e)

    def getting_players_info(self, nodes):
        result = []
        for node in nodes:

            request = parsing_functions.get_request(node.url)
            if request is None: return
            try:
                soup = bs(request.text, 'html.parser')
                content = soup.find('div', 'js-tournament-filter-content').tbody.findAll('tr')
                data = {}
                for item in content:
                    name = item.find(attrs={'class': 'table-item__name'}).text.lstrip().rstrip()
                    role = item.find(attrs={'data-label': 'Амплуа'}).text.lstrip().rstrip()
                    birth = item.find(attrs={'data-label': 'ДР'}).text.lstrip().rstrip()
                    growth = item.find(attrs={'data-label': 'Рост'}).text.lstrip().rstrip()
                    weight = item.find(attrs={'data-label': 'Вес'}).text.lstrip().rstrip()

                    player_url = self.site + item.find('a', 'table-item')['href']
                    request = parsing_functions.get_request(player_url)
                    nationality = bs(request.text, 'html.parser').find('div', text='Гражданство:') \
                        .next_sibling.lstrip().rstrip() if request is not None else Exception

                    data = {'id': None,
                            'team_id': node.data['id'],
                            'name': name,
                            'nationality': nationality,
                            'role': role,
                            'birth': birth,
                            'growth': growth,
                            'weight': weight}
            except Exception as e:
                print('Ошибка парсинга игроков')
                print(e)

            return  data


    def parse_players(self, nodes):
        first_half = nodes[0:len(nodes) // 2]
        second_half = nodes[(len(nodes) // 2):]

        for node in nodes:
            try:
                f1 = self.getting_players_info

                p1 = Process(target=f1, args=(first_half,))
                # p2 = Process(target=tree.parse_players, args=(second_half,))
                # p1.start()
                # p2.start()
                #
                # p1.join()
                # p2.join()
                data = self.getting_players_info(node)

                player = Node(self.ParsingTypes.player, data)
                player.parents.append(node)

                node.add_node(player)

                self.parsed_players += 1
                print('Parsed player:', player.data['name'],' ',player.data['team_id'])

            except Exception as e:
                print('Ошибка парсинга игроков')
                print(e)

    def parse_matches(self, url, parents):
        request = parsing_functions.get_request(url)
        if request is None:
            return
        try:
            soup = bs4.BeautifulSoup(request.text, 'html.parser')
            content = soup.find('div', 'sport__table__tstat').find_all('tr')
            content.pop(0)
            match_nodes = []
            for html_row in content:
                html_columns = html_row.find_all('td', 'sport__table__tstat__td')
                tour = html_columns[0].get_text().lstrip().rstrip()
                match_date = html_columns[1].get_text().lstrip().rstrip()

                teams = html_columns[3].find_all('a')
                home = teams[0].get_text().lstrip().rstrip()
                guest = teams[1].get_text().lstrip().rstrip()

                match_result = html_columns[4].a.get_text().lstrip().rstrip().split(' ')

                penalty_home, penalty_guest, home_result, guest_result = None, None, None, None

                home_result = match_result[0].split(':')[0]
                guest_result = match_result[0].split(':')[1]
                if len(match_result) == 2:
                    if re.match(r"\d:\d", match_result[1]) is not None:#пенальти
                        penalty_home = match_result[1].split(':')[0]
                        penalty_guest = match_result[1].split(':')[1]

                data = {'id': 0,
                        'home_team_id': 0,
                        'guest_team_id': 0,
                        'match_date': match_date,
                        'home_team': home,
                        'guest_team': guest,
                        'home_score': home_result,
                        'guest_score': guest_result,
                        'home_penalty_score': penalty_home,
                        'guest_penalty_score': penalty_guest}

                node = Node(self.ParsingTypes.match, data)

                node.parents.extend(parents)
                for parent in parents: parent.add_player_node(node)

                self.parsed_matches += 1
        except Exception as e:
            print('Ошибка парсинга матчей')
            print(e)

    def parse_seasons(self):
        request = parsing_functions.get_request(self.url)
        if request is None:
            return
        try:
            soup = bs4.BeautifulSoup(request.text, 'html.parser')
            content = soup.find('div', 'js-tournament-header-year').find_all('option')
            if content is None:
                return

            for item in content:
                season_url = self.site+item['data-href']
                html_season_year = item.get_text().lstrip().rstrip().split("/")[0]
                season = datetime.strptime(html_season_year, '%Y')
                season_node = SeasonNode(season, season_url)
                self.root.add_node(season_node)
        except Exception as e:
            print('Ошибка парсинга сезонов')
            print(e)

    # def get_tournaments_with_season(self, year: str):
    #     date = datetime.strptime(year, '%Y') if not isinstance(year, datetime) else year
    #     for season_node in self.root.children:
    #         if season_node.season == date:
    #             return season_node.tournaments
    #     return None

    # def get_tournaments(self):
    #     tournaments = []
    #     season_nodes = self.root.children
    #     for node in season_nodes:
    #         tournaments.extend(node.children)
    #     return tournaments
    #
    # def prepare_tournament_rows(self, tournaments):
    #     tournament_rows = []
    #     for t_node in tournaments:
    #         tournament_rows.append((t_node.name, t_node.country, t_node.start_date, t_node.end_date ))
    #     return tournament_rows
    #
    # def prepare_team_rows(self, teams):
    #     team_rows = []
    #     for team_node in teams:
    #         team_rows.append((team_node.name, team_node.country, team_node.parent.db_id))
    #     return  team_rows
    #
    #
    # def search_node_by_type(self, node, node_type):
    #     if type is None or type(node_type)== node_type:
    #         return node
    #     for child in node.children:
    #         return self.search_node_by_type(child,node_type)
    #
    #
    # def get_season(self, node, year):
    #     date = datetime.strptime(year, '%Y') if not isinstance(year, datetime) else year
    #     if isinstance(node, SeasonNode):
    #         if date is None or node.season == date:
    #             return node
    #
    #     for child in self.root.children:
    #        return self.get_season(child, date)
    #
    # def get_children(self, node):
    #     return node.children
    #
    def get_nodes_by_key(self, key: ParsingTypes, node: Node = None):
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

# def parsing_match_info(url):
#     plays_counter = 0
#     request = parsing_functions.get_request(url)
#     if request is None: return []
#
#     soup = bs4.BeautifulSoup(request.text, 'html.parser')
#     content = soup.find('div','sport__calendar__table').tbody.find_all('tr')
#
#     matches = []
#     for html_row in content:
#         tour = int(html_row.select_one('.sport__calendar__table__tour').get_text())
#         date = datetime.strptime(html_row.select_one('.sport__calendar__table__date').get_text().lstrip().rstrip(), "%d.%m.%Y, %H:%M")
#         home_name = html_row.find('a','sport__calendar__table__team').get_text().lstrip().rstrip()
#         guest_name = html_row.find('a','sport__calendar__table__team').find_next_sibling('a').get_text().lstrip().rstrip()
#
#         home_result = int(html_row.select_one('.sport__calendar__table__result__left').get_text().lstrip().rstrip())
#         guest_result = int(html_row.select_one('.sport__calendar__table__result__right').get_text().lstrip().rstrip())
#
#         matches.append({
#             'tour':tour,
#             'date':date,
#             'home_name':home_name,
#             'guest_name':guest_name,
#             'home_result':home_result,
#             'guest_result':guest_result})
#
#         #match_url = "https://www.championat.com"+html_row.find('td','sport__calendar__table__link').a['href']
#         #match_links.append({'url':match_url,'date':date})
#         plays_counter+=1
#     return matches
#
#
# def parse_plays(plays_url,group_url,playoff_url):
#     match_links = []
#
#     match_links.extend(parsing_match_info(plays_url))
#     match_links.extend(parsing_match_info(group_url))
#     match_links.extend(parsing_match_info(playoff_url))
#
#     return match_links