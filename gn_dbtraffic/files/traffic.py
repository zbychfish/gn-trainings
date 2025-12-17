from random import randint, choice, choices, randrange, sample
from configparser import ConfigParser
from datetime import datetime
from argparse import Namespace
from files.postgres import connect_to_postgres
from files.oracle import connect_to_oracle
from files.shared_defs import is_object, execute_sql, add_customer
from time import sleep
import globals
import string


try:
    from faker import Faker
except ImportError:
    print("Install faker to start \"pip3 install faker\"")
    exit(101)


def set_activity_defaults(conn: object):
    app_schema = globals.GAMES_SCHEMA
    cur = conn.cursor()
    execute_sql(cur, 'SELECT COUNT(*) from {}.customers'.format(app_schema))
    customers_number = cur.fetchone()[0] - 1
    return [customers_number]


def is_time_reached(end_time, check_time=None):
    check_time = datetime.now()
    return True if check_time < end_time else False


def get_cursor(conn: object, config: ConfigParser):
    # global cursor for supported databases
    if config.get('db', 'type') == 'mysql':
        return conn.cursor(buffered=True)
    else:
        return conn.cursor()


def add_cc(config:  ConfigParser, cursor: object, customer_id: str):
    app_schema = globals.GAMES_SCHEMA
    fake = Faker(config.get('settings', 'language'))
    card_provider = choices(['maestro', 'mastercard', 'visa'])
    execute_sql(cursor, "INSERT INTO {s}.credit_cards (customer_id, card_number, card_validity) VALUES ('{ci}','{cc}','{cv}')".format(
        s=app_schema,
        ci=customer_id,
        cc=fake.credit_card_number(card_type=card_provider[0]),
        cv=fake.credit_card_expire(start='now', end='+10y', date_format='%m/%y')
    ))
    execute_sql(cursor, "SELECT COUNT(card_id) FROM {}.credit_cards WHERE customer_id='{}'".format(app_schema, customer_id))
    del fake


def admin_activity_micro_payments(config: ConfigParser, end_time: datetime, args: Namespace):
    app_schema = globals.GAMES_SCHEMA
    while is_time_reached(end_time):
        admin_user = randint(0, len(config.get('settings', 'db_admins').split(',')) - 1)
        admin_tasks = ['pg_settings', 'mistake_sql_error', 'sessions', 'ssl_sessions', 'users_review']
        task_probability = (0.3, 0.01, 0.2, 0.1, 0.1)
        if config.get('db', 'type') == 'postgres':
            if args.v:
                print("Connect to database as administrator: {}".format(config.get('settings', 'db_admins').split(',')[admin_user]))
            app_conn = connect_to_postgres(config, config.get('settings', 'db_admins').split(',')[admin_user], config.get('settings', 'default_password'))
            cursor = get_cursor(app_conn[0], config)
        else:
            print("Incorrect database type")
            exit(0)
        for i in range(randint(0, globals.MAX_TASK_IN_ADMIN_SESSION)):
            session_task = choices(admin_tasks, weights=task_probability, k=1)
            if args.v:
                print("Task {}: {}".format(i, session_task))
            if session_task[0] == 'pg_settings':
                categories = ['Autovacuum', 'Resource Usage%', 'Write-Ahead%', 'Replication%', 'Preset Options', 'Client Connection%', 'Connections And Authentications%',
                              'Statistics%', 'Reporting%', 'Query Tuning%', 'Lock Management', 'Error Handling%']
                sqls = ["select * from pg_settings", "select * from pg_settings where category like '{}'".format(choice(categories)), "select version()"]
                execute_sql(cursor, choice(sqls))
            elif session_task[0] == 'mistake_sql_error':
                sqls = ['select * from pg_settings', 'select version()']
                sql = choice(sqls)
                letter = choice(string.ascii_letters)
                randomIndex = randrange(len(sql))
                err_sql = sql[:randomIndex] + letter + sql[randomIndex:]
                try:
                    cursor.execute(sql)
                except:
                    pass
            elif session_task[0] == 'sessions':
                session_fields = ['usename', 'application_name', 'client_addr', 'backend_type', 'query', 'query_start', 'state_change', 'backend_start', 'pid']
                sqls = ['select count(*) from pg_stat_activity', 'select * from pg_stat_activity', "select {} from pg_stat_activity".format(','.join(sample(session_fields, randint(1, len(session_fields)))))]
                execute_sql(cursor, choice(sqls))
            elif session_task[0] == 'ssl_sessions':
                sqls = ['select * from pg_stat_ssl', "select * from pg_stat_ssl where version != 'TLSv1.3' or version is null", 'select * from pg_stat_ssl where cipher is {}'.format(choice(['not null', 'null'])),
                        "select s.pid, s.ssl, a.usename from pg_stat_ssl s, pg_stat_activity a where s.pid=a.pid{}".format(choice(['', ' and s.cipher is null']))]
                execute_sql(cursor, choice(sqls))
            elif session_task[0] == 'users_review':
                sqls = ['select * from pg_user', 'select usename from pg_user where {} = true'.format(choice(['usecreatedb', 'usesuper', 'userepl']))]
                execute_sql(cursor, choice(sqls))
            #sleep(randint(1, 30))
        sleep(60 * randint(1, 10))
        app_conn[0].close()
        #exit(0)
        pass

def application_traffic_micro_payments(config: ConfigParser, defaults: [int], end_time: datetime, args: Namespace):
    app_schema = globals.GAMES_SCHEMA
    customer_number = defaults[0]
    # defines global internet domain (appears two times in code)
    global_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'mail.com', 'live.com',
                      'icloud.com', 'aol.com', 'protonmail.com', 'zoho.com', 'yandex.com', 'gmx.com', 'me.com',
                      'msn.com', 'mail.ru', 'inbox.ru', 'bk.ru', 'list.ru', 'rediffmail.com', 'qq.com',
                      '126.com', '163.com', 'sina.com', 'yeah.net', 'rocketmail.com', 'fastmail.com',
                      'hushmail.com', 'tutanota.com', 'web.de', 'cox.net', 'comcast.net', 'btinternet.com',
                      'bellsouth.net', 'shaw.ca', 'blueyonder.co.uk', 'virginmedia.com', 'optonline.net',
                      'wanadoo.fr', 'orange.fr', 'seznam.cz']
    if config.get('settings', 'language') == 'pl_PL':
        polish_domains = ['wp.pl', 'onet.pl', 'o2.pl', 'interia.pl', 'gazeta.pl', 'tlen.pl', 'op.pl',
                          'poczta.fm', 'autograf.pl', 'buziaczek.pl', 'vp.pl', 'go2.pl', 'home.pl',
                          'prokonto.pl', 'neostrada.pl', 'amorki.pl', 'konto.pl', 'poczta.onet.pl', 'inetia.pl',
                          'email.pl']
        global_domains.extend(polish_domains)
    # defines how quick sessions are repeated (use specified flag or default settings in config file)
    p_speed = config.get('settings', 'processing_speed') if args.s == None else args.s
    task_timeout = 5 if p_speed == 'slow' else 1 if p_speed == 'normal' else 0.05 if p_speed == 'fast' else 0
    # defines list of activity types to execute and their probability
    tasks_list = ['get_customer_info', 'add_customer', 'add_credit_card', 'buy_feature']
    task_probability = (0.90, 0.04, 0.02, 0.04)
    info_types = ['name_surname', 'email', 'users_from_city', 'has_user_cc', 'extras_per_user', 'features_per_user', 'get_addons_per_user', 'get_extras_per_time', 'get_user_transactions']
    # main loop
    config = ConfigParser()
    config.read('files/config.cfg')
    if bool(int(config.get('game_addons', 'new_constructs'))):
        final_tl = tasks_list
        final_tl.append('new_constructs')
        final_tp = task_probability = task_probability + (0.05,)
    else:
        final_tl = tasks_list
        final_tp = task_probability
    sessions_number = 0
    while is_time_reached(end_time):
        session_steps_number = randint(1, int(config.get('settings', 'maximum_steps_in_session')))
        app_session_user = randint(0, len(config.get('settings', 'app_users').split(',')) - 1)
        if args.v:
            print("Switch context to user {} for {} tasks".format(app_session_user, session_steps_number))
        else:
            sessions_number += 1
            print("\rNumber of sessions: {}".format(sessions_number), end="")
        if config.get('db', 'type') == 'postgres':
            app_conn = connect_to_postgres(config, config.get('settings', 'app_users').split(',')[app_session_user], config.get('settings', 'default_password'))
        if config.get('db', 'type') == 'oracle':
            app_conn = connect_to_oracle(config, config.get('settings', 'app_users').split(',')[app_session_user], config.get('settings', 'default_password'))
        # elif config.get('db', 'type') == 'mysql':
        #     app_conn = connect_to_mysql(config, config.get('settings', 'app_users').split(',')[app_session_user],
        #                                 config.get('settings', 'default_password'))
        else:
            print("unknown database type")
            exit(110)
        for i in range(0, session_steps_number):
            session_task = choices(final_tl, weights=final_tp, k=1)
            if args.v:
                print(session_task[0])
            if session_task[0] == 'new_constructs':
                # new operations to get some user statistics
                app_cursor = get_cursor(app_conn[0], config)
                if config.get('db', 'type') == 'oracle':
                    execute_sql(app_cursor,
                                "SELECT DISTINCT SUBSTR(mail, INSTR(mail, '@') + 1) FROM {}.customers".format(app_schema))
                else:
                    execute_sql(app_cursor, "SELECT DISTINCT substring(mail from position('@' in mail) for length(mail)-position('@' in mail)) FROM {}.customers".format(app_schema))
                execute_sql(app_cursor, "SELECT COUNT(*) FROM {}.customers WHERE mail LIKE '%{}'".format(app_schema, choice(global_domains)))
                execute_sql(app_cursor, "SELECT mail FROM {}.customers WHERE mail LIKE '%{}'".format(app_schema, choice(global_domains)))
                app_cursor.close()
            if session_task[0] == 'get_customer_info':
                if args.v:
                    print('get_customer: ', end="")
                app_cursor = get_cursor(app_conn[0], config)
                if config.get('db', 'type') == 'oracle':
                    execute_sql(app_cursor, "SELECT customer_id FROM {}.customers OFFSET {} ROWS FETCH NEXT 1 ROWS ONLY".format(app_schema, randint(0, customer_number)))
                else:
                    execute_sql(app_cursor, "SELECT customer_id FROM {}.customers LIMIT 1 OFFSET {}".format(app_schema, randint(0, customer_number)))
                get_info_type = choices(info_types, weights=(0.4, 0.2, 0.15, 1, 0.05, 0.05, 0.1, 0.05, 0.05))
                if get_info_type[0] == 'name_surname':
                    result_set = app_cursor.fetchone()
                    fields = ['customer_fname', 'customer_lname', 'city', 'zipcode', 'street']
                    for field in fields:
                        execute_sql(app_cursor, "SELECT {} FROM {}.customers WHERE customer_id='{}'".format(field, app_schema, result_set[0]))
                elif get_info_type[0] == 'email':
                    result_set = app_cursor.fetchone()
                    fields = ['customer_fname', 'customer_lname', 'mail']
                    for field in fields:
                        execute_sql(app_cursor, "SELECT {} FROM {}.customers WHERE customer_id='{}'".format(field, app_schema, result_set[0]))
                    if choice([True, False]):
                        execute_sql(app_cursor, "SELECT COUNT(mail) FROM {}.customers WHERE mail='{}'".format(app_schema, app_cursor.fetchone()[0]))
                elif get_info_type[0] == 'users_from_city':
                    execute_sql(app_cursor, "SELECT city, street FROM {}.customers WHERE customer_id='{}'".format(app_schema, app_cursor.fetchone()[0]))
                    result_set = app_cursor.fetchone()
                    execute_sql(app_cursor, "SELECT COUNT(*) FROM {}.customers WHERE city='{}'".format(app_schema, result_set[0]))
                    execute_sql(app_cursor, "SELECT COUNT(*) FROM {}.customers WHERE city='{}' AND street='{}'".format(app_schema, result_set[0], result_set[1]))
                elif get_info_type[0] == 'has_user_cc':
                    result_set = app_cursor.fetchone()
                    execute_sql(app_cursor, "SELECT COUNT(*) FROM {}.credit_cards WHERE customer_id='{}'".format(app_schema, result_set[0]))
                    if app_cursor.fetchone()[0] != 0:
                        execute_sql(app_cursor, "SELECT card_id, card_number FROM {}.credit_cards WHERE customer_id='{}'".format(app_schema, result_set[0]))
                        execute_sql(app_cursor, "SELECT card_id, card_validity FROM {}.credit_cards WHERE customer_id='{}'".format(app_schema, result_set[0]))
                        execute_sql(app_cursor, "SELECT t.trans_id, t.feature_id, t.extra_id, t.transaction_time, t.price FROM {}.transactions t WHERE t.card_id='{}'".format(app_schema, app_cursor.fetchone()[0]))
                elif get_info_type[0] == 'extras_per_user':
                    result_set = app_cursor.fetchone()
                    execute_sql(app_cursor, "SELECT COUNT(extra_id) FROM {}.transactions WHERE customer_id='{}'".format(app_schema, result_set[0]))
                    if app_cursor.fetchone()[0] != 0:
                        execute_sql(app_cursor, "SELECT e.extra_name, e.extra_price, t.transaction_time FROM {s}.transactions t, {s}.extras e WHERE t.customer_id = '{ex}' AND e.extra_id = t.extra_id".format(s=app_schema, ex=result_set[0]))
                        execute_sql(app_cursor, "SELECT SUM(e.extra_price) FROM {s}.transactions t, {s}.extras e "
                                           "where t.customer_id = '{ex}' AND e.extra_id = t.extra_id".format(s=app_schema, ex=result_set[0]))
                elif get_info_type[0] == 'features_per_user':
                    result_set = app_cursor.fetchone()
                    execute_sql(app_cursor, "SELECT COUNT(feature_id) FROM {}.transactions WHERE customer_id='{}'".format(app_schema, result_set[0]))
                    if app_cursor.fetchone()[0] != 0:
                        execute_sql(app_cursor, "SELECT f.feature_name, f.feature_price, t.transaction_time FROM {s}.transactions t, {s}.features f WHERE "
                                    "t.customer_id = '{ex}' AND f.feature_id = t.feature_id".format(s=app_schema, ex=result_set[0]))
                        execute_sql(app_cursor, "SELECT SUM(f.feature_price) FROM {s}.transactions t, {s}.features f where t.customer_id = '{ex}' AND "
                                    "f.feature_id = t.feature_id".format(s=app_schema, ex=result_set[0]))
                elif get_info_type[0] == 'features_per_user':
                    result_set = app_cursor.fetchone()
                    if app_cursor.fetchone()[0] != 0:
                        execute_sql(app_cursor, "SELECT COUNT(t.transaction_time) FROM {}.transactions t WHERE t.customer_id = '{}'".format(app_schema, result_set[0]))
                        if config.get('db', 'type') == 'postgres':
                            execute_sql(app_cursor, "SELECT e.extra_name, f.feature_name, t.price, t.transaction_time FROM "
                                        "{s}.transactions t  FULL OUTER JOIN {s}.extras e ON "
                                        "e.extra_id = t.extra_id FULL OUTER JOIN {s}.features f ON f.feature_id = t.feature_id WHERE t.customer_id = '{ex}'".format(s=app_schema, ex=result_set[0]))
                            execute_sql(app_cursor, "SELECT SUM(t.price) FROM {s}.transactions t FULL OUTER JOIN {s}.extras e ON e.extra_id = t.extra_id FULL OUTER JOIN "
                                        "{s}.features f ON f.feature_id = t.feature_id WHERE t.customer_id = '{ex}'".format(s=app_schema, ex=result_set[0]))
                        elif config.get('db', 'type') == 'mysql':
                            execute_sql(app_cursor, "SELECT e.extra_name, f.feature_name, t.price, t.transaction_time FROM "
                                        "{s}.transactions t  RIGHT OUTER JOIN {s}.extras e ON e.extra_id = t.extra_id RIGHT OUTER JOIN {s}.features f ON "
                                        "f.feature_id = t.feature_id WHERE t.customer_id = '{ex}'".format(s=app_schema, ex=result_set[0]))
                            execute_sql(app_cursor, "SELECT SUM(t.price) FROM {s}.transactions t RIGHT OUTER JOIN {s}.extras e ON e.extra_id = t.extra_id RIGHT OUTER JOIN "
                                        "{s}.features f ON f.feature_id = t.feature_id WHERE t.customer_id = '{ex}'".format(s=app_schema, ex=result_set[0]))
                        elif config.get('db', 'type') == 'oracle':
                            execute_sql(app_cursor,
                                        "SELECT e.extra_name, f.feature_name, t.price, t.transaction_time FROM "
                                        "{s}.transactions t FULL OUTER JOIN {s}.extras e ON e.extra_id = t.extra_id FULL OUTER JOIN {s}.features f ON "
                                        "f.feature_id = t.feature_id WHERE t.customer_id = '{ex}'".format(s=app_schema, ex=result_set[0]))
                            execute_sql(app_cursor, "SELECT SUM(t.price) FROM {s}.transactions t RIGHT OUTER JOIN {s}.extras e ON e.extra_id = t.extra_id RIGHT OUTER JOIN "
                                        "{s}.features f ON f.feature_id = t.feature_id WHERE t.customer_id = '{ex}'".format(s=app_schema, ex=result_set[0]))
                elif get_info_type[0] == 'get_extras_per_time':
                    period = choices(
                        ['today', 'this_week', 'this_month', 'this_year', 'yesterday', 'last_week', 'last_month',
                         'last_year'], weights=(0.7, 0.3, 0.1, 0.1, 0.1, 0.05, 0.05, 0.05))
                    clause = ''
                    #print("period:", period)
                    if config.get('db', 'type') == 'oracle':
                        if period[0] == 'today':
                            clause = "TRUNC(transaction_time) = TRUNC(SYSDATE)"
                        elif period[0] == 'this_week':
                            clause = "TRUNC(transaction_time, 'IW') = TRUNC(SYSDATE, 'IW')"
                        elif period[0] == 'this_month':
                            clause = "TRUNC(transaction_time, 'MM') = TRUNC(SYSDATE, 'MM')"
                        elif period[0] == 'this_year':
                            clause = "EXTRACT(YEAR FROM transaction_time) = EXTRACT(YEAR FROM SYSDATE)"
                        if period[0] == 'yesterday':
                            clause = "TRUNC(transaction_time) = TRUNC(SYSDATE) - 1"
                        elif period[0] == 'last_week':
                            clause = "transaction_time >= TRUNC(SYSDATE, 'D') - 7 AND transaction_time < TRUNC(SYSDATE, 'D')"
                        elif period[0] == 'last_month':
                            clause = "transaction_time >= TRUNC(ADD_MONTHS(SYSDATE, -1), 'MM') AND transaction_time < TRUNC(SYSDATE, 'MM')"
                        elif period[0] == 'last_year':
                            clause = "transaction_time >= TRUNC(ADD_MONTHS(SYSDATE, -12), 'YYYY') AND transaction_time < TRUNC(SYSDATE, 'YYYY')"
                    else:
                        if period[0] == 'today':
                            clause = "DATE(transaction_time) = CURRENT_DATE"
                        elif period[0] == 'this_week':
                            if config.get('db', 'type') == 'postgres':
                                clause = "DATE_TRUNC('week', DATE(transaction_time)) = DATE_TRUNC('week', CURRENT_DATE)"
                            elif config.get('db', 'type') == 'mysql':
                                clause = "WEEK(transaction_time) = WEEK(CURRENT_DATE()) " \
                                         "AND YEAR(transaction_time) = YEAR(CURRENT_DATE())"
                        elif period[0] == 'this_month':
                            if config.get('db', 'type') == 'postgres':
                                clause = "DATE_TRUNC('month', DATE(transaction_time)) = DATE_TRUNC('month', CURRENT_DATE)"
                            elif config.get('db', 'type') == 'mysql':
                                clause = "MONTH(transaction_time) = MONTH(CURRENT_DATE()) AND " \
                                         "YEAR(transaction_time) = YEAR(CURRENT_DATE())"
                        elif period[0] == 'this_year':
                            if config.get('db', 'type') == 'postgres':
                                clause = "DATE_TRUNC('year', DATE(transaction_time)) = DATE_TRUNC('year', CURRENT_DATE)"
                            elif config.get('db', 'type') == 'mysql':
                                clause = "YEAR(transaction_time) = YEAR(CURRENT_DATE())"
                        if period[0] == 'yesterday':
                            if config.get('db', 'type') == 'postgres':
                                clause = "DATE(transaction_time) = CURRENT_DATE - INTERVAL '1 day'"
                            elif config.get('db', 'type') == 'mysql':
                                clause = "DAYOFYEAR(transaction_time) = DAYOFYEAR(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))"
                        elif period[0] == 'last_week':
                            if config.get('db', 'type') == 'postgres':
                                clause = "transaction_time >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 week') " \
                                         "AND transaction_time < DATE_TRUNC('week', CURRENT_DATE)"
                            elif config.get('db', 'type') == 'mysql':
                                clause = "DAYOFYEAR(transaction_time) <= " \
                                         "DAYOFYEAR(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)) AND " \
                                         "DAYOFYEAR(DATE_SUB(transaction_time, INTERVAL 1 WEEK)) >= " \
                                         "DAYOFYEAR(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK))"
                        elif period[0] == 'last_month':
                            if config.get('db', 'type') == 'postgres':
                                clause = "transaction_time >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') " \
                                        "AND transaction_time < DATE_TRUNC('month', CURRENT_DATE)"
                            elif config.get('db', 'type') == 'mysql':
                                clause = "DAYOFYEAR(transaction_time) <= " \
                                         "DAYOFYEAR(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)) AND " \
                                         "DAYOFYEAR(DATE_SUB(transaction_time, INTERVAL 1 WEEK)) >= " \
                                         "DAYOFYEAR(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK))"
                        elif period[0] == 'last_year':
                            if config.get('db', 'type') == 'postgres':
                                clause = "transaction_time >= DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1 year') " \
                                         "AND transaction_time < DATE_TRUNC('year', CURRENT_DATE)"
                            elif config.get('db', 'type') == 'mysql':
                                clause = "DAYOFYEAR(transaction_time) <= " \
                                         "DAYOFYEAR(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)) AND " \
                                         "DAYOFYEAR(DATE_SUB(transaction_time, INTERVAL 1 WEEK)) >= " \
                                         "DAYOFYEAR(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK))"
                    #print("Clause:", clause)
                    execute_sql(app_cursor, "SELECT SUM(price) FROM {}.transactions WHERE {} AND feature_id IS NOT NULL".format(app_schema, clause))
                    execute_sql(app_cursor, "SELECT SUM(PRICE) FROM {}.transactions WHERE {} AND extra_id IS NOT NULL".format(app_schema, clause))
                    execute_sql(app_cursor, "SELECT SUM(price) FROM {}.transactions WHERE {}".format(app_schema, clause))
                elif get_info_type[0] == 'get_user_transactions':
                    if config.get('db', 'type') == 'postgres':
                        execute_sql(app_cursor, "SELECT t.trans_id, t.transaction_time, e.extra_name, f.feature_name, t.price FROM {s}.transactions t FULL OUTER JOIN {s}.extras e ON "
                                       "e.extra_id = t.extra_id FULL OUTER JOIN {s}.features f ON f.feature_id = t.feature_id where t.customer_id = '{ex}'".format(s=app_schema, ex=app_cursor.fetchone()[0]))
                    elif config.get('db', 'type') == 'mysql':
                        execute_sql(app_cursor, "SELECT t.trans_id, t.transaction_time, e.extra_name, f.feature_name, t.price FROM {s}.transactions t RIGHT OUTER JOIN {s}.extras e ON "
                                                "e.extra_id = t.extra_id RIGHT OUTER JOIN {s}.features f ON f.feature_id = t.feature_id where t.customer_id = '{ex}'".format(s=app_schema, ex=app_cursor.fetchone()[0]))
                if args.v:
                    print(get_info_type[0])
                app_cursor.close()
            elif session_task[0] == 'add_customer':
                app_cursor = get_cursor(app_conn[0], config)
                add_customer(config, app_cursor, global_domains)
                customer_number += 1
                app_cursor.close()
            elif session_task[0] == 'add_credit_card':
                app_cursor = get_cursor(app_conn[0], config)
                if config.get('db', 'type') == 'oracle':
                    execute_sql(app_cursor, "SELECT customer_id FROM {}.customers OFFSET {} ROWS FETCH NEXT 1 ROWS ONLY".format(app_schema, randint(0, customer_number)))
                else:
                    execute_sql(app_cursor, "SELECT customer_id FROM {}.customers LIMIT 1 OFFSET {}".format(app_schema, randint(0, customer_number)))
                add_cc(config, app_cursor, app_cursor.fetchone()[0])
            elif session_task[0] == 'buy_feature':
                app_cursor = get_cursor(app_conn[0], config)
                if config.get('db', 'type') == 'oracle':
                    execute_sql(app_cursor, "SELECT customer_id FROM {}.customers OFFSET {} ROWS FETCH NEXT 1 ROWS ONLY".format(app_schema, randint(0, customer_number)))
                else:
                    execute_sql(app_cursor, "SELECT customer_id FROM {}.customers LIMIT 1 OFFSET {}".format(app_schema, randint(0, customer_number)))
                customer_id = app_cursor.fetchone()[0]
                if is_object(app_cursor, "SELECT COUNT(card_id) FROM {}.credit_cards WHERE customer_id='{}'".format(app_schema, customer_id)) == 0:
                    add_cc(config, app_cursor, customer_id)
                # select card
                if config.get('db', 'type') == 'postgres':
                    execute_sql(app_cursor, "SELECT card_id, card_number, card_validity FROM {}.credit_cards WHERE customer_id='{}' ORDER BY random() LIMIT 1".format(app_schema, customer_id))
                elif config.get('db', 'type') == 'mysql':
                    execute_sql(app_cursor, "SELECT card_id, card_number, card_validity FROM {}.credit_cards WHERE customer_id='{}' ORDER BY rand() LIMIT 1".format(app_schema, customer_id))
                elif config.get('db', 'type') == 'oracle':
                    execute_sql(app_cursor, "SELECT card_id, card_number, card_validity FROM {}.credit_cards WHERE customer_id='{}' ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROWS ONLY".format(app_schema, customer_id))
                credit_card = app_cursor.fetchone()
                validity = credit_card[2].split('/')
                if int(str(datetime.now().year)[2:]) < int(validity[1]) \
                        or (int(str(datetime.now().year)[2:]) == int(validity[1])
                            and int(datetime.now().month) <= int(validity[0])):
                    if choices(['extra', 'feature'], weights=(0.97, 0.03))[0] == 'extra':
                        # select extra
                        if config.get('db', 'type') == 'postgres':
                            execute_sql(app_cursor, "SELECT extra_id, extra_price from {}.extras ORDER BY random() LIMIT 1".format(app_schema))
                        elif config.get('db', 'type') == 'mysql':
                            execute_sql(app_cursor, "SELECT extra_id, extra_price from {}.extras ORDER BY rand() LIMIT 1".format(app_schema))
                        elif config.get('db', 'type') == 'oracle':
                            execute_sql(app_cursor,"SELECT extra_id, extra_price from {}.extras ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROWS ONLY".format(app_schema))
                        extra = app_cursor.fetchone()
                        if args.v:
                            print("Extra: ", extra)
                        execute_sql(app_cursor, "INSERT INTO {}.transactions (extra_id, price, customer_id, card_id) VALUES ('{}', '{}', '{}', '{}')"
                                           .format(app_schema, extra[0], extra[1], customer_id, credit_card[0]))
                    else:
                        # select feature
                        if config.get('db', 'type') == 'postgres':
                            execute_sql(app_cursor, "SELECT feature_id, feature_price from {}.features ORDER BY random() LIMIT 1".format(app_schema))
                        elif config.get('db', 'type') == 'mysql':
                            execute_sql(app_cursor, "SELECT feature_id, feature_price from {}.features ORDER BY rand() LIMIT 1".format(app_schema))
                        elif config.get('db', 'type') == 'oracle':
                            execute_sql(app_cursor,"SELECT feature_id, feature_price from {}.features ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROWS ONLY".format(app_schema))
                        feature = app_cursor.fetchone()
                        if args.v:
                            print(feature)
                        if is_object(app_cursor, "SELECT COUNT(feature_id) FROM {}.transactions WHERE customer_id='{}' AND feature_id='{}'".format(app_schema, customer_id, feature[0])) == 0:
                            execute_sql(app_cursor, "INSERT INTO {}.transactions (feature_id, price, customer_id, card_id) VALUES ('{}', '{}', '{}', '{}')"
                                        .format(app_schema, feature[0], feature[1], customer_id, credit_card[0]))
                        else:
                            if args.v:
                                print("User has feature - transaction cancelled")
                else:
                    if args.v:
                        print("Card expired - transaction rejected")
                # closes main cursor fo session
                app_cursor.close()
            # pause between application sessions
            sleep(task_timeout)
        app_conn[0].close()

