'''
包含生产者的回调，
当每条log的url生成到原主题时，回调就会进行
'''
def delivery_report(err,msg):
    if err is not None:
        print('Message delivery failed:{}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(),msg.partition()))