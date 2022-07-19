import json
import logging
import sys
from os.path import dirname
import pandas as pd
import os

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')

in_log_files = 'log parsing/dataset'

persistence_type = "FILE"
config = TemplateMinerConfig()
config.load(dirname(__file__) + "/drain3.ini")
config.profiling_enabled = True



if persistence_type == "KAFKA":
    from drain3.kafka_persistence import KafkaPersistence

    persistence = KafkaPersistence("skylog_state", bootstrap_servers="localhost:9092")

elif persistence_type == "FILE":
    from drain3.file_persistence import FilePersistence

    persistence = FilePersistence("skylog_state.bin")

elif persistence_type == "REDIS":
    from drain3.redis_persistence import RedisPersistence

    persistence = RedisPersistence(redis_host='',
                                   redis_port=25061,
                                   redis_db=0,
                                   redis_pass='',
                                   is_ssl=True,
                                   redis_key="skylog_state_key")
else:
    persistence = None
#train_file = 'log parsing/dataset/apache_result.csv'

template_miner = TemplateMiner(persistence_handler=persistence,config=config)

def get_allfile(path):
    all_file = []
    for f in os.listdir(path):
        f_name = os.path.join(path,f)
        all_file.append(f_name)
    return all_file
all_files = get_allfile(in_log_files)

for train_file in all_files:
    if not 'pattern' in train_file:
        print(train_file)
        info = pd.read_csv(train_file)
        f = info["log_message"]
        #print(f)
        #with open(train_file,encoding='utf-8') as all_log_file:

        for log_lines in f:
            result = template_miner.add_log_message(log_lines)
            result_json = json.dumps(result)
            print(result_json)
            template = result["template_mined"]
            params = template_miner.extract_parameters(template, log_lines)
            print("Parameters: " + str(params))

print("Training done. Mined clusters:")

for cluster in template_miner.drain.clusters:
    print(cluster)









