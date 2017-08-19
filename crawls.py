import urllib,urllib2
LOGIN_URL=''
LOGIN_EMAIL='example@webscraping.com'
LOGIN_PASSWORD='example'
data={'email':LOGIN_EMAIL,'password':LOGIN_PASSWORD}
encoded_data=urllib.urlencode(data)
request=urllib2.Request(LOGIN_URL,encoded_data)
response=urllib2.urlopen(request)
response.geturl()