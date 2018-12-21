import requests
import common_functions as common
from datetime import datetime

def get_request(url):
    user_agent = common.get_rand_user_agent_from_file("assets/useragents.txt")
    try:
        request = requests.get(url, {'user-agent':user_agent})
        if request.status_code is not 200: return
        return request
    except requests.exceptions.HTTPError as err:
        print('HTTP Error occured')
        print('Response is: {content}'.format(content=err.response.content))
    except requests.exceptions.RequestException as err:
        print('HTTP Error occured')
    except Exception as e:
        print(e)

def log_not_parsing(url, message):
    try:
        current_datetime = datetime.today().strftime("%d_%m_%y-%H_%M_%S")
        log_file = open("logs\\logfile_"+current_datetime+".txt", 'w')

        log_file.write(message)
        log_file.write(datetime.today().strftime("%d-%m-%y %H:%M:%S")+' Not parsing url: '+url)
        log_file.write("/n")
        log_file.close()
    except Exception as e:
        print('Log writing error: '+str(e))
        raise SystemExit()