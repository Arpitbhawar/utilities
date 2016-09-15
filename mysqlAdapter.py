import sys
import time
import MySQLdb
import src.utils.config as cfg
"""
#ReadConfig utility
import os, yaml

def readConfig(name = None):
    HERE = os.path.dirname(os.path.abspath(__file__))
    CONFIG = yaml.load(open(os.path.join(HERE, 'conf', 'filename')))
    return CONFIG
"""
class DataBase():
    def __init__(self, logger = None, name = None):
        configdata = cfg.readConfig(name = name)
        self.config = configdata['DATABASE']
        #print self.config
        self.__connect()
        self.logger = logger

    def __connect(self):
        self.conn = MySQLdb.connect(host=self.config['HOST'], port=self.config.get("PORT", 38036),user=self.config['USER'], passwd=self.config['PASS'], db=self.config['DB'])

    def log(self, msg, level='info'):
        if self.logger:
            ff = getattr(self.logger, level)
            ff(msg)
        else:
            print msg
    def execute(self, query, data = None, process = False, processedFormat = 'lod'):
        def getResults():
            if data is None:
                r = cursor.execute(query)
                print "executing query"
                rr = cursor.fetchall()
                if process:
                    cols = [item[0] for item in cursor.description]
                    res = [dict(zip(cols, row)) for row in rr] if processedFormat == 'lod' else [cols] + list(rr)
                    return res
            else:
                r = cursor.executemany(query, data)
                rr = []
            return cursor.lastrowid,r,rr
        try:
            cursor = self.conn.cursor()
            res = getResults()
        except MySQLdb.OperationalError, oe:
            self.log('Exception||sql||%s||oe %s %s'%(str(oe), query, str(data)))
            if oe[0] in [2006, 2013]: #get MySQL error code
                self.log('Connecting and trying again...')
                self.__connect()
                try:
                    cursor = self.conn.cursor()
                    res = getResults()
                except Exception, e:
                    self.log('Exception||sql||again||%s'%str(e))
                    res = [] if process else (-1, -1, [])
            else:
                res = [] if process else (-1, -1, [])
        except Exception, e:
            self.log('Exception||sql||%s||%s %s'%(str(e), query, str(data)))
            res = [] if process else (-1, -1, [])
        finally:
            cursor.close()
        return res

    def __del__(self):
        try:
            self.conn.close()
        except:
            pass

if __name__=='__main__':
    d = DataBase()
    d.execute(mysqlQuery,process = True)
