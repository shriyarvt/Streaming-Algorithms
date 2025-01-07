import binascii
import time
import sys
import os
from pyspark import SparkContext
import random
from blackbox import BlackBox

#format: f(x) = ((ax + b) % p) % m
#diff variations of (a, b, p)
#num_functions = n/m log2
def myhashs(user):
    p = 1e8 + 7
    res = []
    u = int(binascii.hexlify(user.encode('utf8')), 16)

    for i in range(30):
        a = random.randint(1, 69997)
        b = random.randint(1, 69997)
        hash_value = ((a * u + b) % p) % 69997
        res.append(hash_value)
    return res

def write_output(output_f, res):

    with open(output_f, 'w') as f:
        f.write('Time,FPR\n')
        for time, fpr in zip(res['Time'], res['FPR']):
            f.write(f'{time},{fpr}\n')

def main(input_path, stream_s, num_of_asks, output_path):
    results = {'Time': [], 'FPR': []}

    #track seen users and hash functions
    seen_users = set()
    seen_hashes = set()
    bb = BlackBox()

    for i in range(num_of_asks):
        #get user batch
        stream_users = bb.ask(input_path, stream_s)
        fp = 0
        for user in stream_users:
            result = tuple(myhashs(user))      #get hash results
            if result in seen_hashes:
                if user not in seen_users:
                    fp += 1         #false positive if is user is in seen hashes but not seen users
            seen_hashes.add(result)
            seen_users.add(user)
        results['Time'].append(i)
        results['FPR'].append(fp / stream_s)

    write_output(output_path, results)


if __name__ == "__main__":
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    start = time.time()

    
    input_file = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_asks = int(sys.argv[3])
    output_file = sys.argv[4]
    
    
    # set up
    sc = SparkContext('local[*]', 'BloomFilter')
    sc.setLogLevel('WARN')

    #main
    main(input_file, stream_size, num_asks, output_file)

    # duration
    end = time.time()
    duration = str(end - start)
    print("Duration:", duration)

    sc.stop()

