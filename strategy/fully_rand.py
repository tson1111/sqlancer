import random
import os
os.system("java -jar /Users/bytedance/sqlancer/target/sqlancer-1.1.0.jar \
        --num-tries 100000 \
        --timeout-seconds 600 \
        --print-progress-summary true \
        --num-threads 4 \
        sqlite3 --oracle Subset")
