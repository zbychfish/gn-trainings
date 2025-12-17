from configparser import ConfigParser
from files.postgres import connect_to_postgres, cleanup_schema_postgres, deploy_schema_postgres
from files.oracle import connect_to_oracle, cleanup_schema_oracle, deploy_schema_oracle
from datetime import datetime
from argparse import Namespace
from files.traffic import application_traffic_micro_payments, admin_activity_micro_payments


def connect_to_database(config: ConfigParser, user: str, password: str) -> [object, str]:
    if config.get('db', 'type') in ['postgres']:
        return connect_to_postgres(config, user, password)
    elif config.get('db', 'type') in ['oracle']:
        return connect_to_oracle(config, user, password)
    else:
        print('Unknown database type')


def clean_schema(conn: object, config: ConfigParser, args):
    if config.get('db', 'type') == 'postgres':
        cleanup_schema_postgres(conn, config)
    if config.get('db', 'type') == 'oracle':
        cleanup_schema_oracle(conn, config)
    else:
        print('Unknown database type')


def deploy_schema(conn: object, config: ConfigParser, args):
    if config.get('db', 'type') == 'postgres':
        deploy_schema_postgres(conn, config)
    if config.get('db', 'type') == 'oracle':
        deploy_schema_oracle(conn, config)
    else:
        print('Unknown database type')


def application_traffic(config: ConfigParser, defaults: [int], end_time: datetime, args: Namespace):
    if config.get('db', 'type') in ['postgres', 'mysql', 'oracle']:
        application_traffic_micro_payments(config, defaults, end_time, args)


def admin_activity(config: ConfigParser, end_time: datetime, args: Namespace):
    if config.get('db', 'type') in ['postgres']:
        admin_activity_micro_payments(config, end_time, args)


