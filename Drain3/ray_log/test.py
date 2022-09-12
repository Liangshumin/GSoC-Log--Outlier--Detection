import zlib


def send_data(redis_connection, max_messages: int):
    count = 0
    pipeline = redis_connection.pipeline()
    batch_size = 0
    while count < max_messages:
        batch_size += 1
        data = {
            "producer": 'oap',
            "log_compressed": zlib.compress('- 1117838573 2005.06.03 R02-M1-N0-C:J12-U11\
                 2005-06-03-15.42.53.573391 R02-M1-N0-C:J12-U11 RAS KERNEL INFO\
                  instruction cache parity error corrected'.encode()),  # Just some random data
            "service": 'servicetest',
        }
        pipeline.xadd('test', data)
        count += 1
        if batch_size == 1000:
            # send the pipeline
            batch_size = 0
            resp = pipeline.execute()
            print(resp)
    if batch_size != 0:
        resp = pipeline.execute()
        print(resp)


if __name__ == '__main__':
    from os import environ
    from redis import Redis


    def connect_to_redis():
        hostname = environ.get("REDIS_HOSTNAME", "redis-13894.c290.ap-northeast-1-2.ec2.cloud.redislabs.com")
        port = environ.get("REDIS_PORT", 13894)
        conn = Redis(hostname, port, retry_on_timeout=True, username='default', password='pD0Ukh3KD5f6zAmPbcF1XbSSU8eTLRoi')
        return conn


    conn = connect_to_redis()
    send_data(conn, 10000)
