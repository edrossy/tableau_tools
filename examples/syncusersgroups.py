import sys
reload(sys)
sys.setdefaultencoding('utf8')
import time
from tableau_tools import *
from tableau_tools.tableau_rest_api import *
import logging
import pymssql
import ConfigParser
from sqlalchemy import create_engine, Column, String, Integer, MetaData, Table, inspect

config = ConfigParser.RawConfigParser()
config.read('syncusersgroups.properties')

sqlusr=config.get('DatabaseSection', 'database.sqlusr');
sqlpwd=config.get('DatabaseSection', 'database.sqlpwd');
sqldb=config.get('DatabaseSection', 'database.sqldb');
sqlsvr=config.get('DatabaseSection', 'database.sqlsvr');
sql_statement1=config.get('GroupsSection', 'groups.sql_statement1');
sql_statement2=config.get('UsersSection', 'users.sql_statement2');
delete_from_site=config.get('UsersSection', 'users.delete_from_site');
username=config.get('TableauSection', 'tableau.username');
password=config.get('TableauSection', 'tableau.password');
server=config.get('TableauSection', 'tableau.server');
site=config.get('TableauSection', 'tableau.site');
ver=config.get('TableauSection', 'tableau.ver');

# this will create a daily log file
logfile = 'tableausync_' + str(time.strftime('%Y%m%d')) + '.log'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d|%(levelname)s|%(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S',
                    filename=logfile,
                    filemode='a')
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s|%(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)

logging.info('|START JOB')

connstr = 'DRIVER={SQL Server};SERVER='+sqlsvr+';DATABASE='+sqldb+';UID='+sqlusr+';PWD='+sqlpwd
conn = pymssql.connect(sqlsvr, sqlusr, sqlpwd, sqldb)

metadata = MetaData()
#engine = create_engine(connstr)
engine = create_engine('mssql+pymssql://'+sqlusr+':'+sqlpwd+'@'+sqlsvr+':1433/'+sqldb)
metadata.bind = engine
count = engine.execute(sql_statement1).scalar()

#logger = logging.getLogger('example.log')

t = TableauRestApiConnection(server, username, password, site_content_url=site)
logger = Logger(u"log_file.txt")
t.enable_logging(logger)
t.signin()

# Get all the groups on the Tableau Server
groups = t.query_groups()
groups_dict = t.convert_xml_list_to_name_id_dict(groups)
#print groups_dict

db_connection = engine.connect()
result = db_connection.execute(sql_statement1)
# Loop through the results
db_groups = []
groups_and_users = {}
for row in result:
    db_groups.append(row[0])
    groups_and_users[row[0]] = []
    if row[0] not in groups_dict:
        luid = t.create_group(row[0])
        groups_dict[row[0]] = luid
        logging.info('|Creating GROUP {}'.format(row[0]))
    else:
        continue
for g in groups_dict:
    if g not in db_groups:
        if g == u'All Users':
            continue
        else:
            t.delete_groups(g)
            logging.info('|Deleting GROUP {}'.format(g))
    else:
        continue

# Get all the users on the site
users = t.query_users()
users_dict = t.convert_xml_list_to_name_id_dict(users)

result = db_connection.execute(sql_statement2)
# Loop through users, make sure they exist
for row in result:
    if row[0] not in users_dict:
        logging.info('|Creating USER {}'.format(row[0].encode('utf8')))
        luid = t.add_user(row[0], row[1], site_role=u'Interactor')
        #luid = t.add_user(row[0], row[1], site_role=u'Explorer')
        users_dict[row[0]] = luid

# For the check of who shouldn't be on the server

# List of usernames who should be in the system
usernames = {}
# Add users who are missing from a group

result = db_connection.execute(sql_statement2)
for row in result:
    user_luid = users_dict.get(row[0])
    group_luid = groups_dict.get(row[2])
    #print row[2] + ' (' + str(group_luid) + ')'
    usernames[row[0]] = None
    # Make a data structure where we can check each group that exists on server
    groups_and_users[row[2]].append(row[0])

    logging.info('|Adding user {} to GROUP {} ({})'.format(row[0].encode('utf8'), row[2].encode('utf8'), group_luid.encode('utf8')))
    t.add_users_to_group(user_luid, group_luid)

conn.close()
#print groups_and_users
# Determine if any users are in a group who do not belong, then remove them
for group in groups_and_users:
    if group == groups_dict[u'All Users']:
        continue
    else:
        users_in_group_on_server = t.query_users_in_group(group)
        users_in_group_on_server_dict = t.convert_xml_list_to_name_id_dict(users_in_group_on_server)
        # values() are the LUIDs in these dicts

        for user in users_in_group_on_server_dict.values():
            uname = str(t.convert_xml_list_to_name_id_dict(t.query_user(user)).keys()).replace("['","").replace("']","")
            #gname = str(t.query_group_name(group_luid))
            #print groups_and_users[group]
            group_luid = t.query_group_luid(group)
            user_luid = t.query_user_luid(uname)
            if uname not in groups_and_users[group]:
                if group == u'All Users':
                    logging.info('|Removing user {} from GROUP {}'.format(uname, group))
                    t.remove_users_from_group(uname, group)
                else:
                    logging.info('|Removing user {} from GROUP {}'.format(uname, group))
                    t.remove_users_from_group(user_luid, group_luid)

# Determine if there are any users who are in the system and not in the database, set them to unlicensed
users_on_server = t.query_users()
for user_on_server in users_on_server:
    # Skip the guest user
    if user_on_server.get("name") == 'guest':
        continue
    if user_on_server.get("name") not in usernames:
        if user_on_server.get("siteRole") not in [u'ServerAdministrator', u'SiteAdministrator', u'Publisher', u'Creator', u'SiteAdministratorCreator', u'SiteAdministratorExplorer', u'ExplorerCanPublish']:

            if delete_from_site=="Yes":
                # Remove users from site
                t.remove_users_from_site(user_on_server.get("name"))
                logging.info('|USER on server {} not found in security table...removing from site'.format(
                    user_on_server.get("name").encode('utf8')))
            else:
                # Just set them to 'Unlicensed'
                t.update_user(user_on_server.get("name"), site_role=u'Unlicensed')
                logging.info('|USER on server {} not found in security table, set to Unlicensed'.format(
                    user_on_server.get("name").encode('utf8')))
logging.info('|END JOB')
