import requests
import urllib3
try:
    import psycopg2
except ImportError:
    print("Install psycopg2 to start \"pip3 install psycopg2_binary\"")
    print("This module can require OS packages: postgres, postgres-devel, gcc, python3-devel")
    exit(101)
from configparser import ConfigParser
from files.gdpdefleecher import is_object, connect_to_postgres, get_cursor
from random import randint
from bs4 import BeautifulSoup, element
from datetime import date, datetime, timedelta
from time import sleep


def execute_sql(cur: object, sql: str):
    try:
        cur.execute(sql)
        return cur
    except Exception as err:
        print("SQL Error: ")
        print(sql)
        print(err)
        exit(150)


def is_time_reached(loop_end_time, check_time=None):
    check_time = datetime.now()
    return True if check_time < loop_end_time else False


def chart_traffic(config: ConfigParser, user: str, database: str, verbose: bool, mode, time_execution, task_delay):
    loop_end_time = datetime.now() + timedelta(minutes=time_execution)
    current_task = 1
    if mode == 'normal':
        schema = database
        if config.get('db', 'type') == 'postgres':
            app_conn = connect_to_postgres(config, user, config.get('settings', 'chart_user_password'), database)
        else:
            print('Unknown database type')
            exit(102)
        if app_conn[1] != 'OK':
            print(app_conn[1])
            exit(103)
        app_cursor = get_cursor(app_conn[0], config)
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    else:
        print("Uknown traffic mode")
        exit(103)
    start_date = date(1958, 8, 4)
    while is_time_reached(loop_end_time):
        task_type = randint(0, 13)
        random_date = None
        random_year = None
        random_performer = None
        sql = None
        if verbose:
            print("Current task type {},".format(task_type), "number executed tasks: {}".format(current_task), end="\r", flush=True)
        if task_type in [0, 1]:
            # get random chart release
            random_date = start_date + timedelta(days=randint(0, int((date.today() - start_date).days)))
            sql = "SELECT chart_id FROM {}.charts WHERE chart_issue BETWEEN '{}' AND '{}'".format(schema, random_date, random_date + timedelta(days=6))
            execute_sql(app_cursor, sql)
        if task_type in [2, 3, 4, 5, 6, 11, 13]:
            # get random year
            random_year = datetime.combine(start_date + timedelta(days=randint(0, date.today().year - start_date.year)*365), datetime.time(datetime.today())).year
        if task_type in [7, 8, 9]:
            # get random performer
            sql = "SELECT performer_id FROM {}.performers ORDER BY RANDOM() LIMIT 1".format(schema)
            execute_sql(app_cursor, sql)
            random_performer = app_cursor.fetchone()[0]
        sql = None
        if task_type == 0:
            # get full chart based on date
            sql = ("SELECT po.position, s.name, pe.name FROM {schema}.charts c, {schema}.positions po, {schema}.songs s, {schema}.performers pe "
                   "WHERE c.chart_id = '{chart}' AND po.chart = chart_id AND s.song_id = po.song AND pe.performer_id = s.performer").format(chart=app_cursor.fetchone()[0], schema=schema)
            execute_sql(app_cursor, sql)
        elif task_type == 1:
            # get only new songs from chart
            sql = (
                "SELECT po.position, s.name, pe.name, s.seen_first_time FROM {schema}.charts c, {schema}.positions po, {schema}.songs s, {schema}.performers pe "
                "WHERE c.chart_id = '{chart}' AND po.chart = chart_id AND s.song_id = po.song AND pe.performer_id = s.performer AND c.chart_issue = s.seen_first_time").format(
                chart=app_cursor.fetchone()[0], schema=schema)
        elif task_type == 2:
            # get chart list from random year
            sql = "SELECT c.chart_id, c.url FROM {schema}.charts c WHERE date_part('year', c.chart_issue) = '{year}'".format(schema=schema, year=random_year)
        elif task_type == 3:
            # get only new songs in random year
            sql = "SELECT s.name, s.seen_first_time FROM {schema}.songs s, {schema}.charts c WHERE date_part('year', s.seen_first_time) = '{year}' AND s.seen_first_time = c.chart_issue".format(schema=schema, year=random_year)
        elif task_type == 4:
            # get performers list with new songs in random year
            sql = ("SELECT DISTINCT(pe.name) FROM {schema}.songs s, {schema}.charts c, {schema}.performers pe WHERE "
                   "DATE_PART('year', s.seen_first_time) = '{year}' AND s.seen_first_time = c.chart_issue AND s.performer = pe.performer_id").format(schema=schema, year=random_year)
        elif task_type == 5:
            # get all first positioned songs in random year
            sql = ("SELECT DISTINCT(s.name) FROM {schema}.positions po, {schema}.charts c, {schema}.songs s WHERE "
                   "DATE_PART('year', c.chart_issue) = '{year}' AND po.chart = c.chart_id AND po.position = 1 AND po.song = s.song_id").format(schema=schema, year=random_year)
        elif task_type == 6:
            # get top 10 songs in each chart from random year
            sql = ("SELECT DISTINCT(s.name) FROM {schema}.positions po, {schema}.charts c, {schema}.songs s WHERE "
                   "DATE_PART('year', c.chart_issue) = '{year}' AND po.chart = c.chart_id AND po.position < 11 AND po.song = s.song_id").format(schema=schema, year=random_year)
        elif task_type == 7:
            # get all songs of random performer
            sql = "SELECT s.name, pe.name, s.seen_first_time FROM {schema}.songs s, {schema}.performers pe WHERE pe.performer_id = '{performer}' AND pe.performer_id = s.performer".format(schema=schema, performer=random_performer)
        elif task_type == 8:
            # get best position of all songs of random performer
            sql = ("SELECT s.name AS song_name, MAX(po.position) AS best_position, pe.name  FROM {schema}.songs s, {schema}.performers pe, {schema}.positions po WHERE "
                   "pe.performer_id = '{performer}' AND pe.performer_id = s.performer AND po.song = s.song_id GROUP BY song_name, pe.name ORDER BY best_position").format(schema=schema, performer=random_performer)
        elif task_type == 9:
            # get count of songs of random performer
            sql = "SELECT COUNT(s.name) FROM {schema}.songs s, {schema}.performers pe WHERE pe.performer_id = '{performer}' AND pe.performer_id = s.performer".format(schema=schema, performer=random_performer)
        elif task_type == 10:
            # get song with the longest appearance ever
            sql = "SELECT COUNT(po.song) AS weeks_appearance, s.name AS song_name FROM {schema}.positions po, {schema}.songs s WHERE po.song = s.song_id GROUP BY po.song, song_name ORDER BY weeks_appearance DESC LIMIT 1".format(schema=schema)
        elif task_type == 11:
            # get song with the longest appearance in random year
            sql = ("SELECT COUNT(po.song) AS weeks_appearance, s.name AS song_name, s.song_id FROM {schema}.positions po, {schema}.songs s, {schema}.charts c "
                   "WHERE DATE_PART('year', c.chart_issue) = '{year}' AND po.chart = c.chart_id AND po.song = s.song_id GROUP BY po.song, song_name, s.song_id ORDER BY weeks_appearance DESC LIMIT 1").format(schema=schema, year=random_year)
        elif task_type == 12:
            # get song/s with the highest number of n-th position ever
            sql = ("SELECT COUNT(po.song) AS weeks_on_top, s.name AS song_name from {schema}.positions po, {schema}.songs s "
                   "WHERE po.song = s.song_id AND po.position = {position} GROUP BY po.song, song_name ORDER BY weeks_on_top DESC LIMIT 1").format(schema=schema, position = randint(1, 5))
        elif task_type == 13:
            # get song/s with the highest number n-th position in random year
            sql = ("SELECT COUNT(po.song) AS weeks_on_top, s.name AS song_name from {schema}.positions po, {schema}.songs s, {schema}.charts c "
                   "WHERE po.song = s.song_id AND po.position = {position} AND po.chart = c.chart_id AND DATE_PART('year', c.chart_issue) = '{year}' GROUP BY po.song, song_name ORDER BY weeks_on_top DESC LIMIT 1").format(schema=schema, year=random_year, position = randint(1, 5))
        elif task_type == 100:
            # get best position of all songs of random performer
            sql = ""
        else:
            # error
            print("Incorrect task type")
            exit(103)
        execute_sql(app_cursor, sql)
        current_task += 1
        sleep(task_delay)

    app_cursor.close()
    app_conn[0].close()


def bill_suck(config: ConfigParser, user: str, database: str, verbose: bool):
    #print(user, database)
    schema = database
    if config.get('db', 'type') == 'postgres':
        app_conn = connect_to_postgres(config, user, config.get('settings', 'chart_user_password'), database)
    else:
        print('Unknown database type')
        exit(102)
    if app_conn[1] != 'OK':
        print(app_conn[1])
        exit(103)
    app_cursor = get_cursor(app_conn[0], config)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    base_URL = 'https://www.billboard.com/charts/hot-100/'
    initial_date = '1958-08-04'
    currURL = base_URL + initial_date + '/'
    execute_sql(app_cursor, "SELECT COUNT(*) FROM {}.charts".format(schema))
    if app_cursor.fetchone()[0] == 0:
        fch = True
        prevURL = None
    else:
        execute_sql(app_cursor, "SELECT url FROM {s}.charts ORDER BY chart_issue DESC LIMIT 1".format(s=schema))
        currURL = app_cursor.fetchone()[0]
        fch = False
    is_next = True
    # print(currURL, fch, is_next)
    while is_next:
        page = requests.get(currURL)
        # print(currURL)
        soup = BeautifulSoup(page.content, "html.parser")
        chart_div = soup.find("div", class_='chart-results')
        if chart_div is None:
            print('Page does not provide chart:', currURL)
            next_day = next_day + timedelta(days=1)
            currURL = base_URL + next_day.strftime('%Y-%m-%d') + '/'
        else:
            tag_date = chart_div.find("p", class_='c-tagline').text.lstrip('Week of ')
            chart_date = datetime.strptime(tag_date, '%B %d, %Y').date()
            if base_URL + chart_date.strftime('%Y-%m-%d') + '/' == base_URL + initial_date + '/':
                pass
            elif base_URL + chart_date.strftime('%Y-%m-%d') + '/' == currURL:
                if chart_date >= date.today():
                    print("no more charts")
                    is_next = False
            else:
                currURL = base_URL + chart_date.strftime('%Y-%m-%d') + '/'
            print("Getting chart: {}".format(currURL))
            #print(currURL, fch, is_next)
            if fch:
                if prevURL is None:
                    #execute_sql(app_cursor, "INSERT INTO {}.charts (url, chart_issue) VALUES('{}', '{}')".format(schema, currURL, date(int(chart_date[0:4]), int(chart_date[4:6]), int(chart_date[6:8]))))
                    execute_sql(app_cursor, "INSERT INTO {}.charts (url, chart_issue) VALUES('{}', '{}')".format(schema, currURL, chart_date))
                else:
                    execute_sql(app_cursor, "INSERT INTO {}.charts (url, chart_issue) VALUES('{}', '{}')".format(schema, currURL, chart_date))
                    execute_sql(app_cursor, "UPDATE {}.charts SET next_chart='{}' WHERE url='{}'".format(schema, currURL, prevURL))
            fch = True
            execute_sql(app_cursor, "SELECT chart_id FROM {}.charts WHERE url='{}'".format(schema, currURL))
            chart_id = app_cursor.fetchone()[0]
            tag_songs = (chart_div.find('div', class_='chart-results-list'))
            songs = tag_songs.find_all('ul', class_='o-chart-results-list-row')
            for song in songs:
                columns = song.find_all_next('li', class_='o-chart-results-list__item')
                place = columns[0].findNext('span', class_='c-label').text.strip()
                song_name = columns[3].findNext('h3', class_='c-title').text.strip().replace('"', "'")
                performer = columns[3].findNext('span', class_='c-label').text.strip().replace('"', "'")
                #print(place)
                if '$' in performer:
                    if is_object(app_cursor, "SELECT COUNT(*) FROM {}.performers WHERE name='{}'".format(schema, performer)) == 0:
                        execute_sql(app_cursor, "INSERT INTO {}.performers (name) VALUES ('{}') ON CONFLICT DO NOTHING".format(schema, performer))
                    execute_sql(app_cursor, "SELECT performer_id FROM {}.performers WHERE name='{}'".format(schema, performer))
                else:
                    if is_object(app_cursor, "SELECT COUNT(*) FROM {}.performers WHERE name=$${}$$".format(schema, performer)) == 0:
                        execute_sql(app_cursor, "INSERT INTO {}.performers (name) VALUES ($${}$$) ON CONFLICT DO NOTHING".format(schema, performer))
                    execute_sql(app_cursor, "SELECT performer_id FROM {}.performers WHERE name=$${}$$".format(schema, performer))
                performer_id = app_cursor.fetchone()[0]
                if '$' in song_name:
                    if is_object(app_cursor, "SELECT COUNT(*) FROM {}.songs WHERE name='{}' AND performer='{}'".format(schema, song_name, performer_id)) == 0:
                        execute_sql(app_cursor, "INSERT INTO {}.songs (name, performer, seen_first_time) VALUES ('{}', '{}', '{}')".format(schema, song_name, performer_id, chart_date))
                    execute_sql(app_cursor, "SELECT song_id FROM {}.songs WHERE name='{}' AND performer='{}'".format(schema, song_name, performer_id))
                else:
                    if is_object(app_cursor, "SELECT COUNT(*) FROM {}.songs WHERE name=$${}$$ AND performer='{}'".format(schema, song_name, performer_id)) == 0:
                        execute_sql(app_cursor, "INSERT INTO {}.songs (name, performer, seen_first_time) VALUES ($${}$$, '{}', '{}')".format(schema, song_name, performer_id, chart_date))
                    execute_sql(app_cursor, "SELECT song_id FROM {}.songs WHERE name=$${}$$ AND performer='{}'".format(schema, song_name, performer_id))
                song_id = app_cursor.fetchone()[0]
                execute_sql(app_cursor, "INSERT INTO {}.positions VALUES('{}', {}, '{}') ON CONFLICT DO NOTHING".format(schema, chart_id, int(place), song_id))
            next_day = chart_date + timedelta(days=1)
            #print(next_day)
            prevURL = currURL
            currURL = base_URL + next_day.strftime('%Y-%m-%d') + '/'
    app_conn[0].close()


def uk40(config: ConfigParser, user, database):
    schema = 'uk40'
    app_session_user = randint(0, len(config.get('settings', 'app_users').split(',')) - 1)
    app_conn = connect_to_postgres(config, config.get('settings', 'app_users').split(',')[app_session_user],
                          config.get('settings', 'chartuserpassword'))[0]
    app_cursor = get_cursor(app_conn, config)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    base_URL = 'https://www.officialcharts.com'
    initial_URL = '/charts/singles-chart/19521114/7501/'
    currURL = prevURL = base_URL + initial_URL
    execute_sql(app_cursor, "SELECT COUNT(*) FROM {}.charts".format(schema))
    if app_cursor.fetchone()[0] == 0:
        fch = True
    else:
        execute_sql(app_cursor, "SELECT url FROM {s}.charts ORDER BY chart_issue DESC LIMIT 1".format(s=schema))
        currURL = app_cursor.fetchone()[0]
        fch = False
    is_next = True
    while is_next:
        print("Getting chart: {}".format(currURL))
        page = requests.get(currURL)
        soup = BeautifulSoup(page.content, "html.parser")
        tags_href = soup.find_all("a", href=True)
        tags_items = soup.find_all("div", {"class": "chart-item-content"})
        for href in tags_href:
            next_el = href.next_element
            if isinstance(next_el, element.Tag) and href.next_element.text == 'Next':
                nextURL = base_URL + href['href']
                url_date = currURL.split('/')[5]
                # print(fch, nextURL)
                if fch:
                    execute_sql(app_cursor, "INSERT INTO {}.charts (url, chart_issue) VALUES('{}', '{}')".format(schema, currURL, date(int(url_date[0:4]), int(url_date[4:6]), int(url_date[6:8]))))
                fch = True
                execute_sql(app_cursor, "SELECT chart_id FROM {}.charts WHERE url='{}'".format(schema, currURL))
                chart_id = app_cursor.fetchone()[0]
                # print(nextURL)
                for item in tags_items:
                    song_names = (item.contents[1].find("a", class_="chart-name").find_all("span"))
                    if len(song_names) == 1:
                        song_name = song_names[0].text
                    else:
                        song_name = song_names[1].text
                    #print(song_name)
                    song_href_id = item.contents[1].find("a", class_="chart-name")['href']
                    try:
                        author = item.contents[1].find("a", class_="chart-artist").text
                    except:
                        # neo_session.run("MERGE (m:MISSING {{type: 'performer', song_name: \"{}\", chart: '{}'}})".format(song_name, currURL))
                        # neo_session.run("MERGE (s:SONG {{name: \"{}\", href: '{}'}})".format(song_name, song_href_id))
                        author = "UNKNOWN AUTHOR"
                        position = item.contents[0].find_all("strong")[0].text
                        # neo_session.run("MATCH (s:SONG {{name: \"{}\", href: '{}'}}),(ch:CHART {{url:'{}'}}) MERGE (s)-[:CHARTED_ON {{position: toInteger('{}')}}]->(ch)".format(song_name, song_href_id, currURL, position))
                        continue
                    position = item.contents[0].find_all("strong")[0].text
                    if '$' in author:
                        if is_object(app_cursor, "SELECT COUNT(*) FROM {}.performers WHERE name='{}'".format(schema, author)) == 0:
                            execute_sql(app_cursor, "INSERT INTO {}.performers (name) VALUES ('{}')".format(schema, author))
                        execute_sql(app_cursor, "SELECT performer_id FROM {}.performers WHERE name='{}'".format(schema, author))
                    else:
                        if is_object(app_cursor, "SELECT COUNT(*) FROM {}.performers WHERE name=$${}$$".format(schema, author)) == 0:
                            execute_sql(app_cursor, "INSERT INTO {}.performers (name) VALUES ($${}$$)".format(schema, author))
                        execute_sql(app_cursor, "SELECT performer_id FROM {}.performers WHERE name=$${}$$".format(schema, author))
                    performer_id = app_cursor.fetchone()[0]
                    if is_object(app_cursor, "SELECT COUNT(*) FROM {}.songs WHERE href='{}'".format(schema, song_href_id)) == 0:
                        execute_sql(app_cursor, "INSERT INTO {}.songs (name, href, performer, seen_first_time) VALUES ($${}$$, '{}', '{}', '{}')".format(schema, song_name, song_href_id, performer_id, date(int(url_date[0:4]), int(url_date[4:6]), int(url_date[6:8]))))
                    execute_sql(app_cursor, "SELECT song_id FROM {}.songs WHERE href='{}'".format(schema, song_href_id))
                    song_id = app_cursor.fetchone()[0]
                    if is_object(app_cursor, "SELECT COUNT(*) FROM {}.positions WHERE chart='{}' AND song='{}'".format(schema, chart_id, song_id)) == 0:
                        execute_sql(app_cursor, "INSERT INTO {}.positions VALUES('{}', {}, '{}')".format(schema, chart_id, int(position), song_id))
        if is_next:
            if currURL + '#/' != nextURL:
                execute_sql(app_cursor, "UPDATE {}.charts SET next_chart='{}' WHERE chart_id='{}'".format(schema, nextURL, chart_id))
                prevURL = currURL
                currURL = nextURL
            else:
                is_next = False



