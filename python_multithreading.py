import time
import threading
import math
import pandas as pd


###等分某个大的dataframe ，使用多个线程去处理


def do_something(df):
    for i in df:
        print(i)


def process_in_threads(total_df: pd.DataFrame, target, thread_number=20):
    length = len(total_df.values)

    size = math.ceil(length / thread_number)
    print("There are %s records need to update...\n" % length)
    start_time = time.time()
    threads = []

    for i in range(0, thread_number):
        start = i * size
        end = (i + 1) * size
        if i == thread_number - 1 and end != length:
            end = length

        sub_df = total_df[start: end]

        t = threading.Thread(target=target, args=(sub_df,))
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()

    print('Done, Time cost: %s ' % (end_time - start_time))


def p(thread_index, msg):
    print("Thread %s - %s" % (thread_index, msg))
    pass


if __name__ == '__main__':
    demo_list = []
    for i in range(1000):
        demo_list.append(i)

    df = pd.DataFrame(demo_list)

    process_in_threads(df, do_something)
