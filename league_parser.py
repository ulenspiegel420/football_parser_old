import requests
import bs4

def parse_league(url, user_agent):
    """Парсин лиги. Возвращает строку с названием лиги"""
    request = requests.get(url, {'user-agent':user_agent})
    soup = bs4.BeautifulSoup(request.text, 'html.parser')
    league_name = soup.find('div', 'tournament-header__title-name').get_text().lstrip().rstrip()
    return league_name