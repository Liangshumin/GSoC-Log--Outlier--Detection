import json

import requests
import redis as re

# Establish HTTP connection to OAP
headers = {
    'Authorization': '不记名令牌'
}
params = {
    'expansions': 'attachments.media_keys',
    'media.fields': 'url'
}
r= requests.get(
    'http',
    headers = headers,
    params = params,
    stream = True

)

oaps = r.iter_lines()

#Set up redies producer
redis = {
    'connection':{'bootstrap.servers':'引导程序的redis主题redis的服务器'},
    'producer':{}
}


# Produce message until the process is interrupted
try:
    oap = next(oaps)
    if oap:
        t = json.loads(oap.decode('utf-8'))
        if 'includes' in t and 'media' in t['includes']:
            for log in t['include']['media']:
                if 'media'


except KeyboardInterrupt:
    pass
finally:

