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
       f.write('Time,Ground Truth,Estimation\n')
       for time, gt, est in zip(res['Time'], res['Ground Truth'], res['Estimation']):
           f.write(f'{time},{gt},{est}\n')

def main(input_path, stream_s, num_of_asks, output_path):
    results = {'Time': [], 'Ground Truth':[], "Estimation": []}

    #actual and estimated unique users
    ground_truth = 0
    estimation = 0

    bb = BlackBox()
    for i in range(num_of_asks):
        #get user batch
        stream_users = bb.ask(input_path, stream_s)

        gt = set()  #unique users
        hash_values = [[] for i in range(30)]       #initialize for 30 hash functions

        #get unique counts and hashes
        for user in stream_users:
            gt.add(user)
            user_hashes = myhashs(user)
            for j in range(30):
                hash_values[j].append(user_hashes[j])  #store hashes

        total_est = 0
        #loop through all hash values for each user
        for hash_list in hash_values:
            max_zeroes = 0
            for value in hash_list:
                value = int(value)
                #compare lengths w and without 0s to get num 0s
                zeroes = len(bin(value)[2:]) - len(bin(value)[2:].rstrip('0'))
                max_zeroes = max(max_zeroes, zeroes)        #update max
            total_est += 2 ** max_zeroes          #formula from slides

        total_est = total_est // 30        #divide by num hash functions

        #store results
        ground_truth += len(gt)
        estimation += total_est
        results['Time'].append(i)
        results['Ground Truth'].append(len(gt))
        results['Estimation'].append(total_est)

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
    sc = SparkContext('local[*]', 'FlajoletMartin')
    sc.setLogLevel('WARN')

    #main
    main(input_file, stream_size, num_asks, output_file)

    # duration
    end = time.time()
    duration = str(end - start)
    print("Duration:", duration)

    sc.stop()

