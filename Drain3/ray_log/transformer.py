import logging
import sys
import zlib
from os import environ
from os.path import dirname

import ray

import redis
from redis import Redis

from drain3.masking import LogMasker
from drain3.template_miner_config import TemplateMinerConfig

ray.init()

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')

persistence_type = "REDIS"
config = TemplateMinerConfig()
config.load(dirname(__file__) + "/drain3.ini")


log_masker=LogMasker(config.masking_instructions,config.mask_prefix,config.mask_suffix)




@ray.remote
def get_data(data):
    if not data or not data[0] or not data[0][1]:
        return None,None

    for item in data[0][1]:
        if not item:
            return None,None

        msgId=str(item[0],'utf-8')
        data={}
        for key in item[1].keys():
            k=str(key,'utf-8')
            # print(k)
            try:
                v = str(item[1][key], 'utf-8')
            except Exception:
                val = zlib.decompress(item[1][key])
                v = str(val, 'utf-8')
            data[k]=v
    return msgId,data

@ray.remote(num_cpus=1)
def get_mask(log_body):
    try:

        #处理数据，讲获取到的log_message 和service 进行masking
        mask_content=log_masker.mask(log_body)

        return mask_content



    except Exception as e:
        print('Unable to process log:', e)

@ray.remote(num_cpus=0.05)
class RayConsumer(object):
    # 创建一个stram的consumer group 并连接到redis数据库中
    def __init__(self):
        hostname = environ.get("REDIS_HOSTNAME", "redis-13894.c290.ap-northeast-1-2.ec2.cloud.redislabs.com")
        port = environ.get("REDIS_PORT", 13894)
        self.r= Redis(hostname, port,retry_on_timeout=True, username='default', password='pD0Ukh3KD5f6zAmPbcF1XbSSU8eTLRoi')
        try:
            self.r.xgroup_create('test','ray_group3',id='0')
        except redis.exceptions.ResponseError:
            pass

    def start(self):
        self.run=True

        while self.run:
            # log=self.r.xread('test',count=1,block=None)
            log = self.r.xreadgroup('ray_group3', 'liang', {'test': ">"}, count=1)

            if log is None:
                continue
            id, log_body = ray.get(get_data.remote(log))
            self.r.xack('test','ray_group',id)
            log_message=log_body['log_compressed']
            log_service=log_body['service']
            print(log_message)
            mask = ray.get(get_mask.remote(log_message))
            print(mask)



    def stop(self):
        self.run = False

    def destroy(self):
        self.r.close()

# consumer = RayConsumer.remote()
#
# try:
#     ref = consumer.start.remote()
#     id,body=ray.get(ref)
# except KeyboardInterrupt:
#     consumer.stop.remote()
#
# finally:
#     consumer.destroy.remote()
consumers=[RayConsumer.remote() for _ in range(5)]
#
try:
    refs=[c.start.remote() for c in consumers]
    ray.get(refs)
except KeyboardInterrupt:
    for c in consumers:
        c.stop.remote()
finally:
    for c in consumers:
        c.destroy.remote()

