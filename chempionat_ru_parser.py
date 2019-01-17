from bs4 import BeautifulSoup as bs
import re
import parsing_functions
from datetime import datetime
from enum import Enum
import common_functions as common

SITE_NAME = "https://www.championat.com"


def get_urls_for_parsing(url):
    try:
        content = parsing_functions.get_content(url, 'div', 'js-tournament-header-year')
        if content is None:
            raise Exception('Ошибка получения контента')

        items = content.find_all('option')
        urls = []
        for item in items:
            url = "https://www.championat.com" + item['data-href']
            year = item.text.lstrip().rstrip().split("/")[0]

            data = {'year': year, 'url': url}

            urls.append(data)
        return urls
    except Exception as e:
        raise Exception('Ошибка парсинга ссылок для дальнейшей обработки. ' + str(e))


def parse_tournaments(season):
    request = parsing_functions.get_request(season.data['url'])
    uncovered_items = 0
    try:
        soup = bs(request.text, 'html.parser')
        content = soup.find('div', 'mc-sport-tournament-list').find_all('div', 'mc-sport-tournament-list__item')
        for item in content:
            country = item.find('div', 'item__title').get_text().lstrip().rstrip()
            html_links = item.find_all(attrs={"data-type": "tournament"})
            uncovered_items += len(html_links)
            for html_link in html_links:
                t_name = html_link['data-title'].lstrip().rstrip()
                html_link.find('span', 'separator').extract()
                t_dates_html = html_link.findNext('div', 'item__dates _dates').findAll('span')
                t_start_date = datetime.strptime(t_dates_html[0].get_text().lstrip().rstrip(), "%d.%m.%Y")
                t_end_date = datetime.strptime(t_dates_html[1].get_text().lstrip().rstrip(), "%d.%m.%Y")

                data = {'id': None,
                        'name': t_name,
                        'country': country,
                        'start_date': t_start_date,
                        'end_date': t_end_date,
                        'url': SITE_NAME + html_link['href']}

                tournament = Node(ParsingTypes.tournament, data)
                tournament.set_parent(season)
                season.set_child(tournament)
            print(f'{uncovered_items} tournament(s) uncovered')
    except Exception as e:
        raise Exception('Ошибка парсинга турниров. ' + str(e))


def parse_teams(tournaments):
    uncovered_items = 0
    try:
        for tournament in tournaments:
            url = tournament.data['url'] + 'teams'
            content = parsing_functions.get_contents(url, 'a', 'teams-item__link')
            if content is not None:
                uncovered_items += len(content)
                for item in content:
                    data = {'id': None,
                            'tournament_id': tournament.data['id'],
                            'name': parse_team_name(item),
                            'city': parse_team_city(item),
                            'url': SITE_NAME + item['href']}

                    team = Node(ParsingTypes.team, data)
                    team.set_parent(tournament)
                    tournament.set_child(team)
            else:
                common.logging_warning(f'Ошибка получения контента команды по url: {url}')

        print(f'{uncovered_items} team(s) uncovered')
    except Exception as e:
        print('Ошибка парсинга команд')
        print(e)


def parse_team_name(html_elem):
    player_name_elem = html_elem.find('div', 'teams-item__name')
    if player_name_elem is not None:
        return player_name_elem.text.lstrip().rstrip()


def parse_team_city(html_elem):
    team_city_elem = html_elem.find('div', 'teams-item__country')
    if team_city_elem is not None:
        return team_city_elem.text.lstrip().rstrip()


def parse_players(teams):
    uncovered_items = 0
    try:
        for team in teams:
            url = team.data['url'].replace('result', 'players')
            content = parsing_functions.get_content(url, 'div', 'js-tournament-filter-content')
            if content is not None:
                player_rows = content.tbody.findAll('tr')
                uncovered_items += len(player_rows)
                for item in player_rows:
                    name = parse_player_name(item)
                    role = parse_player_role(item)
                    birth = parse_player_birth(item)
                    growth = parse_player_growth(item)
                    weight = parse_player_weight(item)
                    nationality = parse_player_nationality(item)

                    if name is None or birth is None or growth is None or weight is None or nationality is None:
                        parsing_log(f"Ошибка парсинга игроков: Не получены данные об игроке. url: {url}")
                        continue

                    data = {'id': None,
                            'team_id': team.data['id'],
                            'name': name,
                            'nationality': nationality,
                            'role': role,
                            'birth': birth,
                            'growth': growth,
                            'weight': weight}

                    player = Node(ParsingTypes.player, data)
                    player.set_parent(team)
                    team.set_child(player)
            else:
                parsing_log(f"Ошибка парсинга игроков: Не найден контента для парсинга. url: {url}")
        print(f'{uncovered_items} player(s) uncovered')
    except Exception as e:
        print(f'Ошибка парсинга игроков: ' + str(e))


def parse_player_name(html_elem):
    player_name_elem = html_elem.find(attrs={'class': 'table-item__name'})
    if player_name_elem is not None:
        return player_name_elem.text.lstrip().rstrip()


def parse_player_role(html_elem):
    player_role_elem = html_elem.find(attrs={'data-label': 'Амплуа'})
    if player_role_elem is not None:
        return player_role_elem.text.lstrip().rstrip()


def parse_player_nationality(html_elem):
    player_nationality = '/'.join([country_elem['title'] for country_elem in html_elem.find_all(class_='_country_flag')
                                   if country_elem is not None])
    return player_nationality


def parse_player_birth(html_elem):
    player_birth_elem = html_elem.find(attrs={'data-label': 'ДР'})
    if player_birth_elem is not None:
        return player_birth_elem.text.lstrip().rstrip()


def parse_player_growth(html_elem):
    player_growth_elem = html_elem.find(attrs={'data-label': 'Рост'})
    if player_growth_elem is not None:
        return player_growth_elem.text.lstrip().rstrip()


def parse_player_weight(html_elem):
    player_weight_elem = html_elem.find(attrs={'data-label': 'Вес'})
    if player_weight_elem is not None:
        return player_weight_elem.text.lstrip().rstrip()


def parse_matches(tournaments):
    uncovered_items = 0
    try:
        for tournament in tournaments:
            url = tournament.data['url'] + 'calendar'
            content = parsing_functions.get_content(url, 'table', 'stat-results__table')
            if content is not None:
                rows = content.tbody.find_all('tr')
                uncovered_items += len(rows)
                for html_row in rows:
                    group = parse_match_group(html_row)
                    tour = parse_match_tour(html_row)
                    match_date = parse_match_date(html_row)

                    playing_team_names = parse_match_playing_team_names(html_row)
                    if playing_team_names is None:
                        parsing_log(f"Ошибка парсинга матчей: не удалось получить команды. url: {url}")
                        continue

                    home_team = tournament.search_node(playing_team_names['home'], 'name')
                    if home_team is None:
                        parsing_log(f'Ошибка парсинга матчей: не найдена команда {home_team}. url: {url}')
                        continue

                    guest_team = tournament.search_node(playing_team_names['guest'], 'name')
                    if guest_team is None:
                        parsing_log(f'Ошибка парсинга матчей: не найдена команда {guest_team}. url: {url}')
                        continue

                    home_team_id = home_team.data['id']
                    guest_team_id = guest_team.data['id']

                    main_score = parse_match_main_score(html_row)
                    if main_score is None:
                        parsing_log(f'Ошибка парсинга матчей: не найден счет в матче. url: {url}')
                        continue
                    home_result, guest_result = main_score['home_result'], main_score['guest_result']

                    penalty_home_result, penalty_guest_result = None, None
                    penalty_score = parse_match_penalty_score(html_row)
                    if penalty_score is not None:
                        penalty_home_result = penalty_score['penalty_home_score']
                        penalty_guest_result = penalty_score['penalty_guest_score']

                    is_extra_time = parse_match_is_extra_time(html_row)

                    data = {'id': None,
                            'home_team_id': home_team_id,
                            'guest_team_id': guest_team_id,
                            'group_name': group,
                            'tour': tour,
                            'match_date': match_date,
                            'home_score': home_result,
                            'guest_score': guest_result,
                            'home_penalty_score': penalty_home_result,
                            'guest_penalty_score': penalty_guest_result,
                            'is_extra_time': is_extra_time}

                    match = Node(ParsingTypes.match, data)
                    match.set_parent(tournament)
                    tournament.set_child(match)
            else:
                parsing_log(f'Ошибка парсинга матчей: не найден контент. url: {url}')
        print(f'{uncovered_items} match(es) uncovered')
    except Exception as e:
        print('Ошибка парсинга матчей по url: ' + str(e))


def parse_match_group(html_elem):
    group_elem = html_elem.find('td', 'stat-results__group')
    if group_elem is not None:
        return group_elem.text.lstrip().rstrip()


def parse_match_tour(html_elem):
    tour_elem = html_elem.find('td', 'stat-results__tour-num')
    if tour_elem is not None:
        return tour_elem.text.lstrip().rstrip()


def parse_match_date(html_elem):
    date_elem = html_elem.find('td', 'stat-results__date-time')
    if date_elem is not None:
        return re.sub(r'\s+', ' ', date_elem.text.lstrip().rstrip())


def parse_match_playing_team_names(html_elem):
    team_elements = html_elem.find_all('span', 'stat-results__title-team')
    if len(team_elements) != 2:
        return

    home_elem = team_elements[0].a
    guest_elem = team_elements[1].a

    if home_elem is not None and guest_elem is not None:
        return {'home': home_elem.text.lstrip().rstrip(), 'guest': guest_elem.text.lstrip().rstrip()}


def parse_match_main_score(html_elem):
    score_elem = html_elem.find('span', 'stat-results__count-main')
    if score_elem is not None:
        score = score_elem.text.lstrip().rstrip().split(':')
        return {'home_result': score[0], 'guest_result': score[1]}


def parse_match_penalty_score(html_elem):
    score_elem = html_elem.find('span', 'stat-results__count-ext')
    if score_elem is not None:
        if re.match(r"\d:\d", score_elem.text.lstrip().rstrip()) is not None:
            score = score_elem.text.lstrip().rstrip().split(':')
            return {'penalty_home_result': score[0], 'penalty_guest_result': score[1]}


def parse_match_is_extra_time(html_elem):
    if html_elem.find('span', 'stat-results__count-ext') is not None:
        return True
    else:
        return False


def parsing_log(msg: str):
    with open("logs/parsing.log", 'a') as handle:
            handle.write(datetime.now().strftime("%d.%m.%y|%H:%M:%S")+' '+msg+'\n')


class ParsingTypes(Enum):
    root = 0
    tournament = 1
    team = 2
    player = 3
    match = 4
    season = 5


class Parser:
    def __init__(self):
        self.parsing_tree = None
        self.uncovered_items = 0
        self.parsed_items = 0
        self.parsed_tournaments = 0
        self.parsed_players = 0
        self.parsed_teams = 0
        self.parsed_matches = 0
        self.site = "https://www.championat.com"
        self.seasons = []
        self.tournaments = []
        self.teams = []
        self.players = []
        self.matches = []


class Node:
    def __init__(self, key, data=None):
        self.key = key
        self.data = data
        self.children = []
        self.parents = []

    def get_parents(self):
        return self.parents

    def get_children(self):
        return self.children

    def set_parent(self, node):
        self.parents.append(node)

    def set_child(self, node):
        self.children.append(node)

    def count(self):
        count = 0
        if self.get_children():
            count += len(self.children)
            for child in self.children:
                count += child.count()
        return count

    def count_by_key(self, key: ParsingTypes):
        count = 0
        if self.key is key:
            count += 1
        for child in self.children:
            count += child.count_by_key(key)
        return count

    def get_nodes_by_key(self, key: ParsingTypes):
        if len(self.children) == 0:
            return
        result = []
        for child in self.children:
            if child.key == key:
                result.append(child)
            items = child.get_nodes_by_key(key)
            if items is not None:
                result.extend(items)
        return result

    def search_node(self, value: str, field: str):
        if self.data[field] == value:
            return self

        for child in self.children:
            if child.data[field] == value:
                return child
            else:
                child.search_node(value, field)
