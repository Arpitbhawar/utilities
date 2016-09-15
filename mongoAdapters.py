import pymongo
from pymongo.errors import BulkWriteError
import src.utils.config as cfg
c = cfg.readConfig()

class MongoDb():

    def __init__(self, config = None, logger = None, db = None):
        self.configImp = config if config else c['MONGOIMP']
        self.clientImp = pymongo.MongoClient(self.configImp['HOST'], self.configImp.get('PORT', 27017),socketTimeoutMS =100000, readPreference='secondaryPreferred')
        self.dbImp = self.clientImp[db if db else self.configImp['DATABASE']]

        self.config = config if config else c['MONGO']
        self.client = pymongo.MongoClient(self.config['HOST'], self.config.get('PORT', 27017),socketTimeoutMS =60000)
        self.db = self.client[db if db else self.config['DATABASE']]
        self.logger = logger

    def log(self, msg):
        if self.logger:
            self.logger.info(msg)
        else:
            print msg

    def decreaseUrlImp(self, urls):
        if urls:
            qr = self.db.url.update({'_id': {'$in': urls}, 'imp': {'$gt': c['PARSE']['IMP_THRESHOLD']}}, {'$inc': {'imp': -1}}, multi = True)
            self.log('decreaseUrlImp: %s'%str(qr))

    def bumpUrlImp(self, urls):
        if urls:
            qr = self.db.url.update({'_id': {'$in': url}}, {'$set': {'imp': -20}}, multi = True)
            self.log('bumpUrlImp: %s'%str(qr))

    def updateUrls(self, seller, currDate, data, type):
        bulk = self.db.url.initialize_unordered_bulk_op()
        map(lambda x: bulk.find({'uniqueId': x[1], 'seller': seller}).upsert().update_one({'$set': {'last': currDate, 'imp': 0}, '$setOnInsert': {'_id': x[0], 'first': currDate, 't': type}}), data)
        try:
            r = bulk.execute()
            del r['upserted']
            self.log(str(r))
        except BulkWriteError as bwe:
            self.log('Exception||mongo||updateUrl||%s'%str(bwe.details))

    def deleteUrls(self, urls):
        self.db.url.remove({'_id': {'$in': urls}})

    def getUrls(self, seller, frac, threshold):
        lastUpd = self.db.url.find().sort('last', -1).skip(int(self.db.url.count()*frac) - 1).limit(1)[0]['last']
        urls = self.db.url.find({'seller': seller, 'last': {'$gte': lastUpd}, 'imp': {'$gte': threshold}}, {'uniqueId': 1})
        urlDict = {}
        for url in urls:
            unique = url['uniqueId']
            unique = str(unique.encode('ascii', 'ignore')) if isinstance(unique, unicode) else str(unique)
            urlDict[unique] = url['_id']
        return urlDict

    def insertIntoProcLog(self, date, seller, process, subProc, pid, ppid, state, numExp, num):
        self.db.procLog.insert({'date':date, 'seller':seller, 'process':process, 'subProc':subProc, 'pid':pid, 'ppid':ppid, 'state':state, 'numExp':numExp, 'num':num})

    def __del__(self):
        #self.log("Closing Mongo Connection")
        self.client.close()

if __name=='__main__':
    md = MongoDb().db # Instance created
