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


def get_allfile(path):
    all_file = []
    for f in os.listdir(path):
        f_name = os.path.join(path,f)
        all_file.append(f_name)
    return all_file

def get_log(q:Queue):
    in_log_files = 'log parsing/dataset'
    all_files = get_allfile(in_log_files)
    for train_file in all_files:
        if not 'pattern' in train_file:
            info = pd.read_csv(train_file)
            f = info["log_message"]
            # print(f)
            # with open(train_file,encoding='utf-8') as all_log_file:

            for log_lines in f:
                '''
                如果queue已满，则等待2s之后在存放新的log，如果不满，直接存放log
                '''
                if q.full():
                    q.put(log_lines,timeout=2)
                else:
                    q.put(log_lines)


def get_mask(q:Queue,template_miner:TemplateMiner,mask_list):
    log_list =[]
    while True:
        if q.empty() is not True:
            log_message = q.get()
            log_list.append(log_message)
        else:
            log_message = q.get(timeout=2)
            log_list.append(log_message)
            if False:           # 这个if语句用来判断queue彻底空了，之后也不再进log了
                break
    for log in log_list:
        mask_content = template_miner.get_mask_content(log)
        mask_list.append(mask_content)


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
    queue = Queue()
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
    print("共运行了"+ str(end-start) +"s")
    for m in mask_list:
        print(m)


