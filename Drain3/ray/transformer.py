import json
import logging
import sys
from os.path import dirname
import redis as re
import ray
import requests


from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from ray import delivery_report
from drain3.redis_persistence import RedisPersistence

ray.init()


@ray.remote
class RayProducer:
    def __init__(self, redis):
        #需要修改
        self.r=redis

    def dfwef(self,log_cluster):
        self.r.xadd('stramOUT',log_cluster)


#在getpalette中，应该是把原始的日志信息转换为mask之后的template输出消息
@ray.remote(num_cpus=1)
def get_mask(log):
    try:
        #根据grpc获得数据,获取到log_value，将log分为id，service, log_message
        # value = r.xread()
        log_id = log['id']
        log_service = log['service']
        log_message = log['message']

        #处理数据，讲获取到的log_message 和service 进行masking
        mask_content=TemplateMiner.get_mask_content(log_message)

        return log_id,mask_content,log_service



    except Exception as e:
        print('Unable to process log:', e)


@ray.remote(num_cpus=0.05)
class RayConsumer(object):
    # 创建一个stram的consumer group 并连接到redis数据库中
    def __init__(self,redis,i):
        self.group_name=str(i)+'-logs'
        self.r=redis

        self.r.xgroup_create('streamIN',self.group_name,id='$',mkstream=False)

    def start(self,template_miner):
        self.run=True
        while self.run:
            log = self.r.xreadgroup(self.group_name, 'liang', {'streamIN': ">"}, count=1)
            if log is None:
                continue
            # if msg.error():
            #     print("error:{}".format(msg.error()))
            #     continue
            ray.get(get_mask.remote(log),template_miner)

    def stop(self):
        self.run = False

    def destory(self):
        self.r.connection_pool.disconnect()

@ray.remote
class TemplateMiner:
    def __init__(self):
        logger = logging.getLogger(__name__)
        logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')

        persistence_type = "REDIS"
        config = TemplateMinerConfig()
        config.load(dirname(__file__) + "/drain3.ini")
        config.profiling_enabled = True
        persistence = RedisPersistence(redis_host='',
                                       redis_port=6379,
                                       redis_db=0,
                                       redis_pass='',
                                       is_ssl=True,
                                       redis_key="ray_skylog_state_key")


        self.template_miner = TemplateMiner(persistence_handler=persistence, config=config)

@ray.remote()
class Drain:
    def __init__(self,redis,template_miner):
        self.r=redis
        self.template_miner= template_miner


    def cluster(self,log_mask):
        log_id = log_mask[0]
        mask_content = log_mask[0]
        log_service = log_mask[1]
        result = self.template_miner.get_cluster(mask_content, log_service)
        result_json = json.dumps(result)
        print(result_json)
        template = result["template_mined"]
        # params = template_miner.extract_parameters(template, log_message)
        return {log_id: template}


#启动redis流处理程序
#启动redis，建立连接，
pool = re.ConnectionPool(host='127.0.0.1', port=6379, password="12345", max_connections=1024)
r = re.Redis(connection_pool=pool)
template_miner= TemplateMiner.remote()
comsumers = [RayConsumer.remote(r,i) for i in range(80)]
drain = Drain.remote(template_miner)
#producer = RayProducer.options(name='producer').remote(redis,'palettes')
producer = RayProducer.remote(r)


for c in comsumers:
    c.start.remote(template_miner)

# try:
#     refs = [c.start.remote(template_miner) for c in comsumers ]
#     ray.get(refs)
# except KeyboardInterrupt:
#     for c in comsumers:
#         c.stop.remote()
# finally:
#     for c in comsumers:
#         c.destroy.remote()
#     # producer.destory.remote()
#     # ray.kill(producer)

