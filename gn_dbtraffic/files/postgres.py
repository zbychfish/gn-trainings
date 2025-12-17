from configparser import ConfigParser
from files.shared_defs import is_object, execute_sql, add_customer
try:
    import psycopg2
except ImportError:
    print("Install psycopg2 to start \"pip3 install psycopg2_binary\"")
    print("This module can require OS packages: postgres, postgres-devel, gcc, python3-devel")
    exit(101)
import globals

def connect_to_postgres(config: ConfigParser, user: str, password: str) -> [object, str]:
    try:
        conn = psycopg2.connect("host={} port={} dbname={} user={} password={}".format(
            config.get('db', 'host'),
            config.get('db', 'port'),
            config.get('db', 'database'),
            user,
            password
        ))
        conn.autocommit = True
        error = "OK"
    except Exception as err:
        conn = None
        error = '\nError appeared: {}'.format(err)
    return [conn, error]


def cleanup_schema_postgres(conn: psycopg2._psycopg.connection, config: ConfigParser):
    app_user_group = globals.GAMES_APP_GROUP
    app_admin_group = globals.GAMES_ADMIN_GROUP
    app_schema = globals.GAMES_SCHEMA
    cur = conn.cursor()
    for user in config.get('settings', 'app_users').split(','):
        if is_object(cur, "SELECT COUNT(rolname) FROM pg_roles WHERE rolcanlogin = True AND rolname = '{}'".format(user)) == 1:
            execute_sql(cur, "DROP USER {}".format(user))
    for user in config.get('settings', 'db_admins').split(','):
        if is_object(cur, "SELECT COUNT(rolname) FROM pg_roles WHERE rolcanlogin = True AND rolname = '{}'".format(user)) == 1:
            execute_sql(cur, "DROP USER {}".format(user))
    execute_sql(cur, "DROP SCHEMA IF EXISTS {} CASCADE".format(app_schema))
    execute_sql(cur, "DROP ROLE IF EXISTS {}".format(app_admin_group))
    execute_sql(cur, "DROP ROLE IF EXISTS {}".format(app_user_group))
    cur.close()


def deploy_schema_postgres(conn: psycopg2._psycopg.connection, config: ConfigParser):
    app_user_group = globals.GAMES_APP_GROUP
    app_admin_group = globals.GAMES_ADMIN_GROUP
    app_schema = globals.GAMES_SCHEMA
    cur = conn.cursor()
    # create schema
    # control error here for uuid!
    execute_sql(cur, 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
    execute_sql(cur, "CREATE SCHEMA IF NOT EXISTS {}".format(app_schema))
    if is_object(cur, "SELECT COUNT(rolname) FROM pg_roles WHERE rolname = '{}'".format(app_admin_group)) == 0:
        execute_sql(cur, "CREATE ROLE {}".format(app_admin_group))
        execute_sql(cur, "CREATE ROLE {}".format(app_user_group))
    for user in config.get('settings', 'app_users').split(','):
        if is_object(cur, "SELECT COUNT(rolname) FROM pg_roles WHERE rolcanlogin = True AND rolname = '{}'".format(user)) == 0:
            execute_sql(cur, "CREATE USER {} LOGIN PASSWORD '{}'".format(user, config.get('settings','default_password')))
            execute_sql(cur, "GRANT {} TO {}".format(app_user_group, user))
    for user in config.get('settings', 'db_admins').split(','):
        if is_object(cur, "SELECT COUNT(rolname) FROM pg_roles WHERE rolcanlogin = True AND rolname = '{}'".format(user)) == 0:
            execute_sql(cur, "CREATE USER {} LOGIN PASSWORD '{}'".format(user, config.get('settings','default_password')))
            execute_sql(cur, "GRANT {} TO {}".format(app_admin_group, user))
    # create customers table
    sql = "CREATE TABLE IF NOT EXISTS {}.customers (" \
          "customer_id UUID DEFAULT uuid_generate_v4()," \
          "customer_fname varchar(50)," \
          "customer_lname varchar(50)," \
          "birthday date," \
          "citizen_id varchar(20)," \
          "birth_place varchar(50)," \
          "street varchar(50)," \
          "flat_number varchar(10)," \
          "city varchar(50)," \
          "zipcode varchar(10)," \
          "driving_license varchar(30)," \
          "passport_id varchar(30)," \
          "citizen_doc_id varchar(30)," \
          "mail varchar(50)," \
          "phone varchar(30))".format(app_schema)
    execute_sql(cur, sql)
    execute_sql(cur, "ALTER TABLE {}.customers ALTER COLUMN customer_id SET NOT NULL".format(app_schema))
    execute_sql(cur, "ALTER TABLE {}.customers ADD CONSTRAINT customers_pk PRIMARY KEY (customer_id)".format(app_schema))
    execute_sql(cur, "CREATE TABLE IF NOT EXISTS {s}.credit_cards (card_id UUID DEFAULT uuid_generate_v4(), "
                     "customer_id UUID REFERENCES {s}.customers (customer_id), "
                     "card_number varchar(30), card_validity varchar(12))".format(s=app_schema))
    execute_sql(cur, "ALTER TABLE {}.credit_cards ADD CONSTRAINT cc_pk PRIMARY KEY (card_id)".format(app_schema))
    execute_sql(cur, "CREATE TABLE IF NOT EXISTS {}.features (feature_id UUID DEFAULT uuid_generate_v4(), "
                     "feature_name varchar(40), feature_price real)".format(app_schema))
    execute_sql(cur, "ALTER TABLE {}.features ADD CONSTRAINT features_pk PRIMARY KEY (feature_id)".format(app_schema))
    prices = config.get('game_addons', 'feature_prices').split(',')
    i = 0
    for feature in config.get('game_addons', 'feature_descriptions').split(','):
        execute_sql(cur, "INSERT INTO {}.features (feature_name, feature_price) VALUES ('{}', {})".format(app_schema, feature, prices[i]))
        i += 1
    execute_sql(cur, "CREATE TABLE IF NOT EXISTS {}.extras (extra_id UUID DEFAULT uuid_generate_v4(), extra_name varchar(40), extra_price real)".format(app_schema))
    execute_sql(cur, "ALTER TABLE {}.extras ADD CONSTRAINT extras_pk PRIMARY KEY (extra_id)".format(app_schema))
    prices = config.get('game_addons', 'extra_prices').split(',')
    i = 0
    for feature in config.get('game_addons', 'extra_descriptions').split(','):
        execute_sql(cur, "INSERT INTO {}.extras (extra_name, extra_price) VALUES ('{}', {})".format(app_schema, feature, prices[i]))
        i += 1
    execute_sql(cur, "CREATE TABLE IF NOT EXISTS {s}.transactions ("
                     "trans_id UUID DEFAULT uuid_generate_v4(), "
                     "feature_id UUID REFERENCES {s}.features (feature_id), "
                     "extra_id UUID REFERENCES {s}.extras (extra_id), "
                     "price real, "
                     "customer_id UUID REFERENCES {s}.customers (customer_id), "
                     "card_id UUID REFERENCES {s}.credit_cards (card_id), " 
                     "transaction_time TIMESTAMP DEFAULT now())".format(s=app_schema)
                )
    execute_sql(cur, "GRANT USAGE ON SCHEMA {} TO {}".format(app_schema, app_user_group))
    execute_sql(cur, "GRANT USAGE ON SCHEMA {} TO {}".format(app_schema, app_admin_group))
    execute_sql(cur, "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}".format(app_schema, app_admin_group))
    execute_sql(cur, "GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA {} TO {}".format(app_schema, app_user_group))
    if is_object(cur, "SELECT COUNT(*) FROM {}.customers".format(app_schema)) < config.getint('settings', 'minimal_customer_count'):
        # internet mail domain, second instance this same variable - must be synchronized with
        # definitions.application_traffic
        global_domains = ['gmail.com','yahoo.com','hotmail.com','outlook.com','mail.com','live.com','icloud.com','aol.com','protonmail.com','zoho.com','yandex.com','gmx.com','me.com','msn.com','mail.ru','inbox.ru','bk.ru','list.ru','rediffmail.com','qq.com','126.com','163.com','sina.com','yeah.net','rocketmail.com','fastmail.com','hushmail.com','tutanota.com','web.de','cox.net','comcast.net','btinternet.com','bellsouth.net','shaw.ca','blueyonder.co.uk','virginmedia.com','optonline.net','wanadoo.fr','orange.fr','seznam.cz']
        if config.get('settings', 'language') == 'pl_PL':
            polish_domains = ['wp.pl','onet.pl','o2.pl','interia.pl','gazeta.pl','tlen.pl','op.pl','poczta.fm','autograf.pl','buziaczek.pl','vp.pl','go2.pl','home.pl','prokonto.pl','neostrada.pl','amorki.pl','konto.pl','poczta.onet.pl','inetia.pl','email.pl']
            global_domains.extend(polish_domains)
        for i in range(config.getint('settings', 'minimal_customer_count')):
            add_customer(config, cur, global_domains)
    cur.close()