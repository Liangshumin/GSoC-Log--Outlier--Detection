import logging
import multiprocessing
import sys
import os

import asyncio
import time
import queue

import pandas as pd
from multiprocessing import Queue
from os.path import dirname

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
# 由于masking是一个cpu bound的事件，为了提升速度，使用多进程执行masking


class Node:
    def __init__(self,key,value):
        self.log_value = key
        self.log_service = value

class LogQueue():
    def __init__(self,maxsize=0):
        self.queue = Queue(maxsize)

    def get(self,block=True, timeout=None):
        if self.queue.empty():
            return None
        else:
            return self.queue.get(block, timeout)

    def put(self,obj,block=True, timeout=None):
        if self.queue.full():
            return None
        else:
            return self.queue.put(obj,block,timeout)



def get_allfile(path):
    all_file = []
    for f in os.listdir(path):
        f_name = os.path.join(path,f)
        all_file.append(f_name)
    return all_file

def get_log(q:LogQueue):
    in_log_files = 'log parsing/dataset'
    all_files = get_allfile(in_log_files)
    for train_file in all_files:
        if not 'pattern' in train_file:
            log_service = train_file
            info = pd.read_csv(train_file)
            f = info["log_message"]
            # print(f)
            # with open(train_file,encoding='utf-8') as all_log_file:

            for log_lines in f:
                '''
                如果queue已满，则等待2s之后在存放新的log，如果不满，直接存放log
                '''
                log_node= Node(log_lines,log_service)
                q.put(log_node)

                # if q.full():
                #     q.put(log_node,timeout=2)
                # else:
                #     q.put(log_node)


def get_mask(q:LogQueue,template_miner:TemplateMiner,mask_list):
    log_list =[]
    log_node = q.get()
    while log_node is not True:
        log_list.append(log_node)
        log_message = log_node.log_value
        log_service = log_node.log_service
        mask_content = template_miner.get_mask_content(log_message)
        mask_node = Node(mask_content, log_service)
        mask_list.append(mask_node)
        log_node = q.get()

def get_cluster(q:LogQueue):
    mask_node:Node = q.get()
    while mask_node is not None:
        mask_content = mask_node.log_value
        log_service = mask_node.log_service
        result = template_miner.get_cluster(mask_content,log_service)




if __name__ =='__main__':
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')


    persistence_type = "FILE"
    config = TemplateMinerConfig()
    config.load(dirname(__file__) + "/drain3.ini")
    config.profiling_enabled = True

    if persistence_type == "KAFKA":
        from drain3.kafka_persistence import KafkaPersistence

        persistence = KafkaPersistence("multi_skylog_state", bootstrap_servers="localhost:9092")

    elif persistence_type == "FILE":
        from drain3.file_persistence import FilePersistence

        persistence = FilePersistence("multi_skylog_state.bin")

    elif persistence_type == "REDIS":
        from drain3.redis_persistence import RedisPersistence

        persistence = RedisPersistence(redis_host='',
                                       redis_port=25061,
                                       redis_db=0,
                                       redis_pass='',
                                       is_ssl=True,
                                       redis_key="multi_skylog_state_key")
    else:
        persistence = None

    template_miner = TemplateMiner(persistence_handler=persistence, config=config)
    queue = LogQueue()

    log_list =[]
    mask_list =[]

    log_input = multiprocessing.Process(target=get_log(queue))
    mask = multiprocessing.Process(target=get_mask(queue,template_miner,mask_list))
    start = time.time()
    log_input.start()
    mask.start()

    log_input.join()
    mask.join()
    end = time.time()



