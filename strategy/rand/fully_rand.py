import os
import random
import subprocess
import time

iter_num = 400
jar_path = "/Users/bytedance/sqlancer/target/sqlancer-1.1.0.jar"
num_tries = "100000"
timeout_seconds = "30"
options_path = "/Users/bytedance/sqlancer/strategy/options"
print_progress_summary = "true"
num_threads = "4"
redirect_file = "test_for_now" + str(iter_num)
log_file_path = "log_iter" + str(iter_num) +  "_timeout" + str(timeout_seconds)

N_DIM = 14

run_sqlancer_str = "java -jar {} --num-tries {} --timeout-seconds {} --options-path {} --print-progress-summary {} --num-threads {} sqlite3 --oracle Subset >> {}"

# init
option = [0.5]*N_DIM
with open(options_path, "w") as f:
    f.write(str(0.5))
    for i in range(N_DIM - 1):
        f.write(',')
        f.write(str(0.5))

def write_option_to_file(a):
    with open(options_path, "w") as f:
        f.write(str(a[0]))
        for i in range(N_DIM - 1):
            f.write(',')
            f.write(str(a[i+1]))

def get_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def log_write(a):
    global log_file_path
    with open(log_file_path, "a") as f:
        f.write(a)
        f.write("\n")

# iterate
for iter_cnt in range(iter_num):
    print("[{}]Start iteration #{}".format(get_time(), iter_cnt))
    log_write("[{}]Start iteration #{}".format(get_time(), iter_cnt))
    log_write("[{}]options for iteration #{}: {}".format(get_time(), iter_cnt, option))
    os.system(run_sqlancer_str.format(jar_path, num_tries, timeout_seconds, options_path, print_progress_summary, num_threads, redirect_file))
    stats = [int(i) for i in subprocess.getoutput("tail -1 " + redirect_file).split(',')]
    score = stats[0]/1000 + stats[1]*1000
    log_write("[{}]Queries: {}, Bugs: {} for iteration #{}".format(get_time(), stats[0], stats[1], iter_cnt))
    print("[{}]Score for iteration #{}: {}".format(get_time(), iter_cnt, score))
    log_write("[{}]Score for iteration #{}: {}".format(get_time(), iter_cnt, score))
    for i in range(N_DIM):
        option[i] = random.random()
    with open(options_path, "w") as f:
        f.write(str(option[0]))
        for i in range(1, N_DIM):
            f.write(',')
            f.write(str(option[i]))
