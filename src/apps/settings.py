'''
overall settings for the app

by: dan trepanier
    Trep Capital

date: September 30, 2023
'''

import os

def get_stem_dir():
    current_dir = os.getcwd()
    lst = current_dir.split('/')
    new = []
    for x in lst:
        new += [x]
        if x == 'pantheon':
            break
    return '/'.join(new) + '/'


# server info
SERVERS = {'vancouver': '192.168.1.238',
           'montreal': '192.168.1.240',
           'calgary': '192.168.1.122',
           'toronto': '192.168.1.239',
           
           'sydney':'192.168.1.237',
           'halifax':'192.168.1.242',
           'moncton':'192.168.1.243',
           'gander': '192.168.1.201',
           'quebec': '192.168.1.241',
          }
       

# folder info
DIR = get_stem_dir()
LOG_DIR = DIR + 'logs/'
INCOMING_DIR = DIR + 'data/incoming/'

# redis info
REDIS_CONF = {'host':'192.168.1.238', 'port':6379, 'db':0}
REDIS_CONF_SIMS = {'host':'192.168.1.240', 'port':6379, 'db':0}

# postgres info
DB_HOST             = '192.168.1.239'
DB_USER             = 'machine'
DB_NAME             = 'trepcapital'
DB_PASSWORD         = 'Opt!plex'

# ib settings
IB_CONF = {'host':'192.168.1.237', 
           'port':7495,
           'account': 'DU2451911',
           'user_id': 'dptrep123',
            }

IB_CONF_SIMS = {'host':'192.168.1.83',
                'port':7496, 
                'account': 'DDU1011026',
                'user_id': 'dptrep123',
            }
# if live:
IB_SWITCH = {True: IB_CONF,
             False: IB_CONF_SIMS}

# polygon settings
POLYGON_KEY = "hXWzmnDY7oPvNMdIyGpmGapeLBd8c9oY"

# Google oauth settings
GOOGLE_OAUTH_FILE = DIR + 'data/google/credentials.json'
GOOGLE_OAUTH_CLIENT_ID = '1025952711027-m3rtnc31nd4te486rmrv72bfkc3rt7n0.apps.googleusercontent.com'
GOOGLE_CLIENT_SECRET = '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' 


# nepune settings
NEPTUNE_API_TOKEN = "eyJhcGlfYWRkcmVzcyI6Imh0dHBzOi8vYXBwLm5lcHR1bmUuYWkiLCJhcGlfdXJsIjoiaHR0cHM6Ly9hcHAubmVwdHVuZS5haSIsImFwaV9rZXkiOiI4ZTg4YjQ5Ni1iYzc0LTQzNzItOTU2MS02MGRhNWZjYTEzMmQifQ=="

# email settings
MAIL = {'port':465,
        'smtp_server':'smtp.gmail.com',
        'user':'trepcapital@gmail.com',
        'password':'hetwneupwhswebzd',
        }