import numpy as np
import os
import subprocess
import time


N_DIM = 14
iter_num = 20
iter_cnt = 0
size_pop = 20
jar_path = "/Users/bytedance/sqlancer/target/sqlancer-1.1.0.jar"
num_tries = "100000"
timeout_seconds = "30"
options_path = "/Users/bytedance/sqlancer/strategy/options"
print_progress_summary = "true"
num_threads = "4"
redirect_file = "GA_test_stats"
run_sqlancer_str = "java -jar {} --num-tries {} --timeout-seconds {} --options-path {} --print-progress-summary {} --num-threads {} sqlite3 --oracle Subset >> {}"
log_file_path = "log_iter" + str(iter_num) + "_pop" + str(size_pop) + "_timeout" + str(timeout_seconds)

# helper function
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



# init
p = np.array([0.5]*14)
write_option_to_file(p)


def step(p):
    '''
    This function has plenty of local minimum, with strong shocks
    global minimum at (0,0) with value 0
    '''
    global jar_path, num_tries, timeout_seconds, options_path, print_progress_summary, num_threads, redirect_file, iter_cnt
    iter_cnt += 1
    print("[{}]Start iteration #{}".format(get_time(), iter_cnt))
    log_write("[{}]Start iteration #{}".format(get_time(), iter_cnt))
    log_write("[{}]options for iteration #{}: {}".format(get_time(), iter_cnt, p))
    write_option_to_file(p)
    os.system(run_sqlancer_str.format(jar_path, num_tries, timeout_seconds, options_path, print_progress_summary, num_threads, redirect_file))
    stats = [int(i) for i in subprocess.getoutput("tail -1 " + redirect_file).split(',')]
    score = stats[0]/1000 + stats[1]*1000
    log_write("[{}]Queries: {}, Bugs: {} for iteration #{}".format(get_time(), stats[0], stats[1], iter_cnt))
    print("[{}]Score for iteration #{}: {}".format(get_time(), iter_cnt, score))
    log_write("[{}]Score for iteration #{}: {}".format(get_time(), iter_cnt, score))
    return -score

# %%
from sko.GA import GA

ga = GA(func=step, n_dim=N_DIM, size_pop=size_pop, max_iter=iter_num, lb=np.zeros(N_DIM), ub=np.ones(N_DIM), precision=1e-7)
best_x, best_y = ga.run()
print('best_x:', best_x, '\n', 'best_y:', best_y)
log_write("best_x: " + str(best_x))
log_write("best_y: " + str(best_y))
# %% Plot the result
import pandas as pd
# import matplotlib.pyplot as plt

Y_history = pd.DataFrame(ga.all_history_Y)
Y_history.to_csv("Y_history_csv")
# fig, ax = plt.subplots(2, 1)
# ax[0].plot(Y_history.index, Y_history.values, '.', color='red')
# Y_history.min(axis=1).cummin().plot(kind='line')
# plt.show()