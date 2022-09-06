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

ray.init()
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')

persistence_type = "Redis"
config = TemplateMinerConfig()
config.load(dirname(__file__) + "/drain3.ini")
config.profiling_enabled = True

if persistence_type == "KAFKA":
    from drain3.kafka_persistence import KafkaPersistence

    persistence = KafkaPersistence("ray_skylog_state", bootstrap_servers="localhost:9092")

elif persistence_type == "FILE":
    from drain3.file_persistence import FilePersistence

    persistence = FilePersistence("ray_skylog_state.bin")

elif persistence_type == "REDIS":
    from drain3.redis_persistence import RedisPersistence

    persistence = RedisPersistence(redis_host='',
                                   redis_port=6379,
                                   redis_db=0,
                                   redis_pass='',
                                   is_ssl=True,
                                   redis_key="ray_skylog_state_key")
else:
    persistence = None

template_miner = TemplateMiner(persistence_handler=persistence, config=config)

@ray.remote
class RayProducer:
    def __init__(self, redis, sink):
        #需要修改
        import redis as re
        self.producer = Producer({**redis['connection'], **redis['producer']})
        self.sink = sink

    def produce(self, palette):
        self.producer.produce(self.sink, json.dumps(palette).encode('utf-8'), callback=delivery_report)
        self.producer.poll(0)

    def destroy(self):
        self.producer.flush(30)

#在getpalette中，应该是把原始的日志信息转换为mask之后的template输出消息
@ray.remote(num_cpus=1)
def get_palette(log):
    try:
        #根据grpc获得数据,获取到log_value，将log分为id，service, log_message
        # value = r.xread()
        log_id = log['id']
        log_service = log['service']
        log_message = log['message']

        #处理数据，讲获取到的log_message 和service 进行masking
        mask_content=TemplateMiner.get_mask_content(log_message)

        return (mask_content,log_service)



    except Exception as e:
        print('Unable to process log:', e)

@ray.remote(num_cpu=1)
def get_cluster(log_mask):
    try:
        mask_content = log_mask[0]
        log_service= log_mask[1]
        result = template_miner.get_cluster(mask_content,log_service)
        result_json = json.dumps(result)
        print(result_json)
        template = result["template_mined"]
        #params = template_miner.extract_parameters(template, log_message)
    except Exception as e:
        print('Unable to process cluster:',e)


@ray.remote(num_cpus=0.05)
class RayConsumer(object):
    # 创建一个stram的consumer group 并连接到redis数据库中
    def __init__(self,redis,i):

        self.r= redis.xgroup_create()

    def start(self):
        self.run=True
        while self.run:
            log = self.r.xread
            if log is None:
                continue
            # if msg.error():
            #     print("error:{}".format(msg.error()))
            #     continue
            ray.get(get_palette.remote(log))

    def stop(self):
        self.run = False

    def destory(self):
        self.r.connection_pool.disconnect()
#给定一个redis的结构
redis = {
    'connection':{},

}
#启动redis流处理程序
#启动redis，建立连接，
pool = re.ConnectionPool(host='127.0.0.1', port=6379, password="12345", max_connections=1024)
r = re.Redis(connection_pool=pool)
comsumers = [RayConsumer.remote(r,i) for i in range(80)]
#producer = RayProducer.options(name='producer').remote(redis,'palettes')


try:
    refs = [c.start.remote() for c in comsumers ]
    ray.get(refs)
except KeyboardInterrupt:
    for c in comsumers:
        c.stop.remote()
finally:
    for c in comsumers:
        c.destroy.remote()
    # producer.destory.remote()
    # ray.kill(producer)

