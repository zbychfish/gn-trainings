# import argparse
from configparser import ConfigParser
# import random
# import datetime
# import string
# import time
# import globals


try:
    import psycopg2
except ImportError:
    print("Install psycopg2 to start \"pip3 install psycopg2_binary\"")
    print("This module can require OS packages: postgres, postgres-devel, gcc, python3-devel")
    exit(101)


# checks connection to postgres database
def connect_to_postgres(config: ConfigParser, user: str, password: str, database: str, app_name) -> [object, str]:
    try:
        conn = psycopg2.connect("host={} port={} dbname={} user={} password={} application_name={}".format(
            config.get('db', 'host'),
            config.get('db', 'port'),
            database,
            user,
            password,
            app_name
        ))
        conn.autocommit = True
        error = "OK"
    except Exception as err:
        conn = None
        error = err
    return [conn, error]


def get_cursor(conn: object, config: ConfigParser):
    # global cursor for supported databases
    if config.get('db', 'type') == 'mysql':
        return conn.cursor(buffered=True)
    else:
        return conn.cursor()


def is_object(cursor: object, sql: str) -> int:
    # cursor.execute(sql)
    execute_sql(cursor, sql)
    return cursor.fetchone()[0]


# executes SQL using cursor and prints SQL exception error, cursor type depends on database type
def execute_sql(cur: object, sql: str):
    try:
        cur.execute(sql)
        return cur
    except Exception as err:
        print("SQL Error: ")
        print(sql)
        print(err)
        exit(150)

def deploy_schema_postgres(conn: psycopg2._psycopg.connection, config: ConfigParser, source: str, user: str):
    cur = conn.cursor()
    # create schema
    # control error here for uuid!
    execute_sql(cur, 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
    execute_sql(cur, "CREATE SCHEMA IF NOT EXISTS {}".format(source))
    if is_object(cur, "SELECT COUNT(rolname) FROM pg_roles WHERE rolname = 'leecher'") == 0:
        execute_sql(cur, "CREATE ROLE leecher")
    if is_object(cur, "SELECT COUNT(rolname) FROM pg_roles WHERE rolcanlogin = True AND rolname = '{}'".format(user)) == 0:
        execute_sql(cur, "CREATE USER {} LOGIN PASSWORD '{}'".format(user, config.get('settings','chart_user_password')))
        execute_sql(cur, "GRANT leecher TO {}".format(user))
    # create customers table
    if source in ['uk40', 'billboard']:
        sql = "CREATE TABLE IF NOT EXISTS {s}.charts (chart_id UUID DEFAULT uuid_generate_v4(), url text UNIQUE, chart_issue date UNIQUE, next_chart text)".format(
            s=source)
    elif source == 'lp3':
        sql = "CREATE TABLE IF NOT EXISTS {s}.charts (chart_id UUID DEFAULT uuid_generate_v4(), url text UNIQUE, chart_number integer, next_chart text, chart_issue date UNIQUE)".format(
            s=source)
    elif source in ['radioszczecin', 'rmf']:
        sql = "CREATE TABLE IF NOT EXISTS {s}.charts (chart_id integer, release date)".format(s=source)
    execute_sql(cur, sql)
    execute_sql(cur, "ALTER TABLE {}.charts ADD CONSTRAINT chart_pk PRIMARY KEY (chart_id)".format(source))
    if source in ['uk40', 'billboard']:
        execute_sql(cur,
                    "CREATE TABLE IF NOT EXISTS {}.performers (performer_id UUID DEFAULT uuid_generate_v4(), name text UNIQUE)".format(
                        source))
    elif source in ['lp3', 'radioszczecin', 'rmf']:
        execute_sql(cur,
                    "CREATE TABLE IF NOT EXISTS {}.performers (performer_id UUID DEFAULT uuid_generate_v4(), name text, href text UNIQUE)".format(
                        source))
    execute_sql(cur, "ALTER TABLE {}.performers ADD CONSTRAINT performer_pk PRIMARY KEY (performer_id)".format(
        source))
    if source in ['uk40', 'radioszczecin']:
        execute_sql(cur,
                    "CREATE TABLE IF NOT EXISTS {s}.songs (song_id UUID DEFAULT uuid_generate_v4(), name text, href text UNIQUE, performer UUID REFERENCES {s}.performers (performer_id), seen_first_time date)".format(
                        s=source))
    elif source in ['rmf']:
        execute_sql(cur,
                    "CREATE TABLE IF NOT EXISTS {s}.songs (song_id UUID DEFAULT uuid_generate_v4(), name text, performer UUID REFERENCES {s}.performers (performer_id), seen_first_time integer, UNIQUE (song_id, performer))".format(
                        s=source))
    elif source in ['lp3']:
        execute_sql(cur,
                    "CREATE TABLE IF NOT EXISTS {s}.songs (song_id UUID DEFAULT uuid_generate_v4(), name text, href text UNIQUE, performer UUID REFERENCES {s}.performers (performer_id), seen_first_time integer)".format(
                        s=source))
    elif source == 'billboard':
        execute_sql(cur,
                    "CREATE TABLE IF NOT EXISTS {s}.songs (song_id UUID DEFAULT uuid_generate_v4(), name text, performer UUID REFERENCES {s}.performers (performer_id), seen_first_time date)".format(
                        s=source))
    execute_sql(cur, "ALTER TABLE {}.songs ADD CONSTRAINT song_pk PRIMARY KEY (song_id)".format(source))
    if source in ['radioszczecin', 'rmf']:
        execute_sql(cur,
                    "CREATE TABLE IF NOT EXISTS {s}.positions (chart integer REFERENCES {s}.charts (chart_id), position integer, song UUID REFERENCES {s}.songs (song_id), UNIQUE (chart, position, song))".format(
                        s=source))
    else:
        execute_sql(cur,
                    "CREATE TABLE IF NOT EXISTS {s}.positions (chart UUID REFERENCES {s}.charts, position integer, song UUID REFERENCES {s}.songs (song_id), UNIQUE (chart, position, song))".format(
                        s=source))
    execute_sql(cur, "GRANT USAGE ON SCHEMA {} TO leecher".format(source))
    execute_sql(cur, "GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA {} TO leecher".format(source))
    execute_sql(cur, "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO leecher".format(source))
    cur.close()


def cleanup_schema_postgres(conn: psycopg2._psycopg.connection, config: ConfigParser, source: str, user: str):
    cur = conn.cursor()
    if is_object(cur,"SELECT COUNT(rolname) FROM pg_roles WHERE rolcanlogin = True AND rolname = '{}'".format(user)) == 1:
        execute_sql(cur, "DROP USER {}".format(user))
    execute_sql(cur, "DROP SCHEMA IF EXISTS {} CASCADE".format(source))
    # execute_sql(cur, "DROP ROLE IF EXISTS leecher")
    cur.close()