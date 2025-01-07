import time
import sys
import os
from pyspark import SparkContext
import random
from blackbox import BlackBox

def write_output(output_f, res):
   with open(output_f, 'w') as f:
       f.write('seqnum,0_id,20_id,40_id,60_id,80_id\n')
       for n, u1, u2, u4, u6, u8 in zip(res['seqnum'],
                res['0_id'],
                res['20_id'],
                res['40_id'],
                res['60_id'],
                res['80_id']):
           f.write(f'{n},{u1},{u2},{u4},{u6},{u8}\n')

def main(input_path, stream_s, num_of_asks, output_path):
    results = {'seqnum': [], '0_id':[], "20_id": [], '40_id': [], '60_id': [], '80_id': []}

    bb = BlackBox()
    users = []
    user_count = 0

    for i in range(num_of_asks):
        # get user batch
        stream_users = bb.ask(input_path, stream_s)

        for user in stream_users:
            user_count += 1

            # add user if length less than 100
            if len(users) < 100:
                users.append(user)
            else:
                #accept sample if randomnum < s/n
                r = random.random()
                if r < 100 / user_count:
                    idx = random.randint(0, 99)
                    users[idx] = user           #replace at random index

            #add every 100th
            if user_count % 100 == 0:
                results['seqnum'].append(user_count)
                results['0_id'].append(users[0])
                results['20_id'].append(users[20])
                results['40_id'].append(users[40])
                results['60_id'].append(users[60])
                results['80_id'].append(users[80])

    write_output(output_path, results)


if __name__ == "__main__":
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    start = time.time()
    random.seed(553)

    
    input_file = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_asks = int(sys.argv[3])
    output_file = sys.argv[4]
    
    # set up
    sc = SparkContext('local[*]', 'FixedSizeSampling')
    sc.setLogLevel('WARN')

    #main
    main(input_file, stream_size, num_asks, output_file)

    # duration
    end = time.time()
    duration = str(end - start)
    print("Duration:", duration)

    sc.stop()

