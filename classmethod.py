import urllib2
from pymongo import MongoClient
import random
import urlparse

class Download:
    def __init__(self,throttle=None,proxies=None,headers=None,user_agent='wspa',cache=None):
        self.throttle=throttle 
        self.proxies=proxies 
        self.user_agent=user_agent
        self.cache=cache
        self.headers=headers
    
    def __call__(self,url):
        pass  
    
    
    def download(self,url,num_retries=2):
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
    
    
                
        
    