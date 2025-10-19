#!/usr/bin/python3

import pymysql.cursors
from _creds import my_user, my_pass
from flowpy.utils import setup_logger
from config_loader import ConfigLoader

logfn = '../log/'+__name__+'.log'
from logging import CRITICAL, DEBUG, INFO
logger = setup_logger(__name__, logfn, level=DEBUG)


class StorageHandler: #(CsvTransformer):
    def __init__(self):
        self.init_db()
    def init_db(self):
        self.conn = pymysql.connect(
            host='localhost',
            #host='zrz-ux-vm10745-a.rz-sued.bayern.de',
            user='infra',
            password=my_pass,
            database='infra',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

    def writerow(self, row):
        self.store(row)

    def store(self, row):
        #logger.debug(row)
        #vals = row.values()
        vals = []
        keys = ['Servername', 'Betriebssystem', 'IP Adresse']
        keys2= ['Schutzzone', 'Cluster', 'app_ger_env_id']
        for k in keys: vals.append(row[k])
        logger.debug(vals)
        with self.conn.cursor() as cursor:
            sql = "SELECT app_ger_env_id FROM `hostgroup` WHERE `app_ger_env_id` = %s"
            res = cursor.execute(sql, row['app_ger_env_id'])
            if not res:
                vals = [row[k] for k in keys2]
                logger.debug("missing: %s - %s", row['app_ger_env_id'], res)
                sql = "INSERT INTO `hostgroup` (`Schutzzone`, `Cluster`, `app_env_ger_id`) VALUES (%s, %s, %S)"
                num = cursor.execute(sql, vals)

            vals.append(res)
            sql = "INSERT INTO `host` (`Servername`, `Betriebssystem`, `IP_Adresse`, `hostgroup_id) VALUES (%s, %s, %s, %s)"
            num = cursor.execute(sql, vals)

            #logger.debug('num rows: %s', num)
        self.conn.commit()
        self.conn.close()

#if __name__ == '__main__':
#    sh = StorageHandler()
#    sh.work()



