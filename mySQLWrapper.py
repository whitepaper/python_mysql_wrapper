# import mysql.connector
# import MySQLdb as mysql
import pymysql as mysql
import logging
import time


class MySQLWrapper:
    def __init__(self, userName, password, host, dbName):
        # self.config = {'user': userName, 'password': password, 'host': host, 'database': dbName}
        # self.conn = mysql.connect(**self.config)

        self.userName = userName
        self.password = password
        self.host = host
        self.dbName = dbName

        self.conn = mysql.connect(host=self.host, user=self.userName, passwd=self.password,
                                  db=self.dbName, charset='utf8mb4')

    def executeQuery(self, query):
        while(True):
            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                result = cursor.fetchall()
                result = [list(x) for x in result]
                self.conn.commit()
                cursor.close()
                return result
            except (SystemExit, KeyboardInterrupt):
                raise
            except:
                logging.error('Failed to open file', exc_info=True)
                logging.error(query)
                time.sleep(5)
                raise
                # self.conn = mysql.connect(host=self.host, user=self.userName, passwd=self.password,
                #                          db=self.dbName, charset='utf8mb4')

    def executeQueryParams(self, query, params):
        while(True):
            try:
                cursor = self.conn.cursor()
                cursor.execute(query, params)
                result = cursor.fetchall()
                result = [list(x) for x in result]
                self.conn.commit()
                cursor.close()
                return result
            except (SystemExit, KeyboardInterrupt):
                raise
            except:
                logging.error('Failed to open file', exc_info=True)
                logging.error(query)
                time.sleep(5)
                raise
                # self.conn = mysql.connect(host=self.host, user=self.userName, passwd=self.password,
                #                           db=self.dbName, charset='utf8mb4')

    def executeInsertOrUpdate(self, sql):
        while(True):
            try:
                cursor = self.conn.cursor()
                cursor.execute(sql)
                self.conn.commit()
                cursor.close()
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except:
                logging.error('Failed to open file', exc_info=True)
                logging.error(sql)
                time.sleep(5)
                raise
                # self.conn = mysql.connect(host=self.host, user=self.userName, passwd=self.password,
                #                           db=self.dbName, charset='utf8mb4')

    def executeInsertOrUpdateParams(self, sql, params):
        while(True):
            try:
                cursor = self.conn.cursor()
                cursor.execute(sql, params)
                self.conn.commit()
                cursor.close()
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except:
                logging.error('Failed to open file', exc_info=True)
                logging.error(sql)
                time.sleep(5)
                raise
                # self.conn = mysql.connect(host=self.host, user=self.userName, passwd=self.password,
                #                           db=self.dbName, charset='utf8mb4')

    def execute(self, sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        self.conn.commit()
        cursor.close()

    def createTable(self, sql):
        self.execute(sql)

    def executeBatch(self, sql, data):
        cursor = self.conn.cursor()
        cursor.executemany(sql, data)
        self.conn.commit()
        cursor.close()

    def dropTable(self, tableName):
        sql = "drop table if exists {}".format(tableName)
        self.execute(sql)

    def truncateTable(self, tableName):
        sql = "truncate {}".format(tableName)
        self.execute(sql)

    def checkTableExists(self, dbName, tableName):
        query = ("SELECT count(*) from INFORMATION_SCHEMA.TABLES "
                 "WHERE table_schema = '{0}' AND table_name = '{1}'")
        query = query.format(dbName, tableName)

        rows = self.executeQuery(query)
        count = rows[0][0]
        if count == 0:
            return False
        else:
            return True

    def create_tmp_id_table(self, table_name, ids):
        self.dropTable(table_name)
        sql = "CREATE TABLE {} (id BIGINT, INDEX(id))".format(table_name)
        self.createTable(sql)

        if ids is not None and len(ids) > 0:
            ids = [str(x) for x in ids]
            sql_values = ','.join(['(' + x + ')' for x in ids])
            sql = "INSERT INTO {} (id) VALUES ".format(table_name) + sql_values
            self.executeInsertOrUpdate(sql)

    def create_tmp_id_pair_table(self, table_name, ids):
        self.dropTable(table_name)
        sql = "CREATE TABLE {} (id1 BIGINT, id2 BIGINT, INDEX(id1, id2))".format(table_name)
        self.createTable(sql)

        if ids is not None and len(ids) > 0:
            ids = [','.join(map(str, x)) for x in ids]
            sql_values = ','.join(['(' + x + ')' for x in ids])
            sql = "INSERT INTO {} (id1, id2) VALUES ".format(table_name) + sql_values
            self.executeInsertOrUpdate(sql)

    def insert_row(self, table_name, row_dict):
        keys = row_dict.keys()
        columns = ", ".join(keys)
        values_template = ", ".join(["%s"] * len(keys))
        values = tuple(row_dict[key] for key in keys)

        cursor = self.conn.cursor()
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, columns, values_template)
        cursor.execute(sql, values)
        self.conn.commit()
        cursor.close()

    def close(self):
        self.conn.close()
