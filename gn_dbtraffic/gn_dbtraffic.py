import datetime
import argparse
from configparser import ConfigParser
from files.common import connect_to_database, clean_schema, deploy_schema, application_traffic, admin_activity
from files.traffic import set_activity_defaults

# , set_activity_defaults, clean_schema, deploy_schema, application_traffic

# Parses input parameters
parser = argparse.ArgumentParser()
config = ConfigParser()
config.read('files/config.cfg')
parser.add_argument('-v', help='Verbose output', required=False, action='store_false')
parser.add_argument('-a', help='action', default='app_flow', choices=['app_flow', 'clean', 'schema', 'rebuild', 'attack', 'admins'])
parser.add_argument('-t', help='execution time in minutes', default=120, type=int)
parser.add_argument('-s', help='speed', choices=['slow', 'normal', 'fast', 'insane'])
c_args = parser.parse_args()
if c_args.v:
    print("Connection to: ", config.get('db', 'type'))
# Checks supported databases
if config.get('db', 'type') in ['postgres', 'oracle']:
    conn = connect_to_database(config, config.get('db', 'user'), config.get('db', 'password'))
else:
    print('No supported database in config')
    exit(102)
if conn[1] != 'OK':
    print('Connection problem {}'.format(conn[1]))
    exit(103)
# Defines when script will stop activity
end_time = datetime.datetime.now() + datetime.timedelta(minutes=c_args.t)
print('Script will finish execution:', end_time)
# Selects appropriate actions to execute
if c_args.a in ['clean', 'rebuild']:
    clean_schema(conn[0], config, c_args)
if c_args.a in ['schema', 'rebuild']:
    deploy_schema(conn[0], config, c_args)
if c_args.a in ['app_flow']:
    session_defaults = set_activity_defaults(conn[0])
    application_traffic(config, session_defaults, end_time, c_args,)
if c_args.a in ['admins']:
    admin_activity(config, end_time, c_args)
# if c_args.a == 'attack':
#     print("functionality not implemented yet ...")
