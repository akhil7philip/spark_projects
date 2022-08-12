import time
import pandas as pd

from helper_funcs.producer import Producer

sink_topic          = 'test_248'
bootstrap_servers   = 'localhost:9092'

# transform data using pandas 
# and publish using kafka
def transform_publish_records(rdd):
    if rdd.isEmpty():
        print("waiting for data")
    else:
        start_time  = time.perf_counter()
        records     = rdd.collect()
        df          = pd.DataFrame(records)
        '''
        do transformations here
        '''
        records     = list(df.apply(lambda x:x.to_dict(), axis=1))

        o           = Producer(sink_topic, [bootstrap_servers])
        for r in records:
            o.send_message(
                key     = None,\
                value   = r)
        o.flush_message()
        end_time    = time.perf_counter()        
        time_taken  = f'{end_time - start_time}s'
        print('time taken transform and push msg: '+ time_taken)
        print('---------------------------------------------')