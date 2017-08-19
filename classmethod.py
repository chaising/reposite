import urllib2
from pymongo import MongoClient,errors
import random
import urlparse
import logging
from datetime import datetime , timedelta

logging.basicConfig(level=logging.DEBUG,format="[%(asctime)s] (%(threadName)s) %(message)s",)

class Download:
    def __init__(self,throttle=None,proxies=None,num_retries,headers=None,user_agent='wspa',cache=None):
        self.throttle=throttle 
        self.proxies=proxies 
        self.user_agent=user_agent
        self.cache=cache
        self.headers=headers
        self.num_retries=num_retries
    
    def __call__(self,url):
        results=None
        if self.cache:
            try:
                results=self.cache['url']
            except KeyError:
                pass 
            else:
                if self.num_retries>0 and 500<=results['code']<600:
                    results=None
        if results is None:
            results=self.download(url,self.num_retries)
            if self.cache:
                self.cache[url]=results
        return results['html']
    
    
    def download(self,url,num_retries=2):
        logging.debug("Downloading...%s",url)
        headers=self.headers or {}
        if self.user_agent:
            headers['User-agent']=self.user_agent
        request=urllib2.Request(url,headers=headers)
        opener=urllib2.ProxyHandler()
        proxy=random.choice(self.proxies) if self.proxies else None
        if proxy:
            param={urlparse.urlparse(url).scheme:proxy}
            opener.build_opener(param)
        try:
            response=opener.open(request)
            html=response.read()
            code=response.code 
        except Exception as e:
            html=""
            if hasattr(e,'code'):
                code=e.code 
                if num_retries>0 and 500<=code<600:
                    self.download(url, num_retries-1)
        return {'html':html,'code':code}
    
    
class MongoQueue:
    OUTSTANDING,PROCESSING,COMPLETED=range(3)
    def __init__(self,client=None,timeout=300):
        self.client=MongoClient("mongodb://localhost:27017/") if client is None else client
        self.db=self.client.datas 
        self.timeout=timeout
    
    def __nonzero__(self):
        record=self.db.coll.find_one({'status':{'$ne':self.COMPLETED}})
        return True if record else False
    
    def push(self,url):
        try:
            self.db.coll.insert({'_id':url,'status':self.OUTSTANDING})
        except errors.DuplicateKeyError:
            pass  
    
    def pop(self):
        record=self.db.coll.find_and_modify(query={'status':self.OUTSTANDING},update={'$set':{'status':self.PROCESSING,'timestamp':datetime.now()}})
        if record:
            return record['_id']
        else:
            self.repair()
            raise KeyError()    #This is details that show the last uncompleted url in mongodb
    
    def complete(self,url):
        self.db.coll.update({'_id':url},{'$set':{'status':self.COMPLETED}})
        
    def repair(self):
        
        record=self.db.coll.find_and_modify(query={'timstamp':{'$lt':datetime.now()-timedelta(seconds=self.timeout)},'status':{'$ne':self.COMPLETED}},
                                            update={'$set':{'status':self.OUTSTANDING}})
        if record:
            logging.debug('Released:%s',record['_id'])
            


