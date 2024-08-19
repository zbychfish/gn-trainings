from argparse import ArgumentParser
from configparser import ConfigParser
from files.gdpcommonleecher import connect_to_database, suck, traffic, deploy_schema, clean_schema

parser = ArgumentParser()
config = ConfigParser()
config.read('files/gdptraining.cfg')
parser.add_argument('-v', help='verbose output', required=False, action='store_true')
parser.add_argument('-a', help='action', default='suck', required=False, choices=['suck', 'clean', 'schema', 'traffic'])
parser.add_argument('-d', help='chart', default='billboard', required=False, choices=['billboard'])
parser.add_argument('-t', help='execution time in minutes', default=120, type=int)
parser.add_argument('-m', help='traffic mode', default='normal', choices=['normal', 'app_extended'])
parser.add_argument('-s', help='speed', default='normal', choices=['slow', 'normal', 'fast', 'insane'])
c_args = parser.parse_args()

if c_args.v: print("Connection to:", config.get('db', 'type'), 'on', config.get('db', 'host') + ':' + config.get('db', 'port'))
task_timeout = 5 if c_args.s == 'slow' else 1 if c_args.s == 'normal' else 0.05 if c_args.s == 'fast' else 0.005 # last is insane
# checks supported databases
if config.get('db', 'type') in ['postgres']:
    conn = connect_to_database(config, config.get('db', 'user'), config.get('db', 'password'), c_args.d, '')
else:
    print('No supported database in config')
    exit(102)
if conn[1] != 'OK':
    print('Connection problem {}'.format(conn[1]))
    exit(103)
# appropriate actions to execute
if c_args.a in ['clean']:
    clean_schema(conn[0], config, c_args.d)
if c_args.a in ['schema']:
    deploy_schema(conn[0], config, c_args.d)
if c_args.a in ['suck']:
    suck(config, c_args.d, c_args.v)
if c_args.a in ['traffic']:
    traffic(config, c_args.d, c_args.v, c_args.m, c_args.t, task_timeout)

# close connection to database
conn[0].close()
