from configparser import ConfigParser
from files.gdpdefleecher import connect_to_postgres, cleanup_schema_postgres, deploy_schema_postgres
from files.gdpleecher import bill_suck, bill_traffic, uk40
from datetime import datetime


def traffic(config: ConfigParser, database: str, verbose: bool, mode, time: datetime, speed):
    print('Start traffic generation charts for', database) if verbose else ''
    if database == 'billboard':
        bill_traffic(config, config.get('settings', 'chart_user'), database, verbose, mode, time, speed)
    else:
        print("Unknown chart provider:", database)


def suck(config: ConfigParser, database: str, verbose: bool):
    print('Start synchronize charts for', database) if verbose else ''
    if database == 'billboard':
        bill_suck(config, config.get('settings', 'chart_user'), database, verbose)
    #elif database == 'uk40':
    #     uk40(config, config.get('settings', 'chart_user'), database)
    else:
        print("Unknown chart provider:", database)


def connect_to_database(config: ConfigParser, user: str, password: str, database: str) -> [object, str]:
    supported_databases=['postgres']
    if config.get('db', 'type') in supported_databases:
        return connect_to_postgres(config, user, password, database)


def clean_schema(conn: object, config: ConfigParser, database: str):
    if config.get('db', 'type') == 'postgres':
        cleanup_schema_postgres(conn, config, database, config.get('settings', 'chart_user'))


def deploy_schema(conn: object, config: ConfigParser, database: str):
    if config.get('db', 'type') == 'postgres':
        deploy_schema_postgres(conn, config, database, config.get('settings', 'chart_user'))

