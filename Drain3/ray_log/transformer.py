import json
import logging
import sys
import zlib
from os import environ
from os.path import dirname

import ray

import redis
from cachetools import LRUCache
from redis import Redis

from drain3.drain import Drain
from drain3.masking import LogMasker
from drain3.template_miner import LogCache
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
            # print(mask)
            drain = ray.get_actor('drain')
            result=ray.get(drain.get_cluster.remote(mask,log_service))
            result_json=json.dumps(result)
            print(result_json)



    def stop(self):
        self.run = False

    def destroy(self):
        self.r.close()



@ray.remote
class DrainCluster:
    def __init__(self):
        param_str = config.mask_prefix + "*" + config.mask_suffix
        self.drain=Drain(
            sim_th=config.drain_sim_th,
            depth=config.drain_depth,
            max_children=config.drain_max_children,
            max_clusters=config.drain_max_clusters,
            max_logs=config.drain_max_logs,
            extra_delimiters=config.drain_extra_delimiters,
            param_str=param_str,
            parametrize_numeric_tokens=config.parametrize_numeric_tokens
        )
        self.num_mask=0
        self.log_cache=LogCache(self.drain.max_logs)
        self.log_cluster_cache = LogCache(self.drain.max_logs)
        self.parameter_extraction_cache = LRUCache(self.config.parameter_extraction_cache_capacity)

    def get_cluster(self,mask_content,log_service):
        mask_id = self.log_cache.get(mask_content)
        if mask_id is None:

            self.log_cache[mask_content] = self.num_mask


            # 根据log_service确定用那个tree进行搜索
            cluster, change_type = self.drain.add_log_message(mask_content, log_service)
            self.log_cluster_cache[self.num_mask] = cluster


        else:
            cluster = self.log_cluster_cache.get(mask_id)
            cluster.size += 1
            self.drain.id_to_cluster[cluster.cluster_id]
            change_type = "none"

        result = {
            "change_type": change_type,
            "cluster_id": cluster.cluster_id,
            "cluster_size": cluster.size,
            "template_mined": cluster.get_template(),
            "cluster_count": len(self.drain.clusters)
        }
        return result

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
drain = DrainCluster.options(name='drain').remote()

try:
    refs=[c.start.remote() for c in consumers]
    ray.get(refs)
except KeyboardInterrupt:
    for c in consumers:
        c.stop.remote()
finally:
    for c in consumers:
        c.destroy.remote()

