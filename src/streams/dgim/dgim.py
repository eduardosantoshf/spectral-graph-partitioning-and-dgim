from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import math
import sys

global WINDOW_SIZE

class Bucket:
    def __init__(self, ts, ones):
        self.ones = ones
        self.final_ts = ts

    # merge two buckets and its ones count 
    def __add__(self, second_bucket):
        self.final_ts = max(self.final_ts, second_bucket.final_ts)
        self.ones += second_bucket.ones

        return self

# queue struct to merge buckets
class Queue:
    def __init__(self):
        self.buckets = [[]]

    def push(self, bucket):
        self.buckets[0].insert(0, bucket)
        self._merge_buckets()

    def _merge_buckets(self):
        for i in range(len(self.buckets)):
            if len(self.buckets[i]) > 2:
                try:
                    # merge the last two buckets in the current bucket group
                    merged_bucket = self.buckets[i].pop() + self.buckets[i].pop()
                    # insert the merged bucket at the beginning of the next bucket group
                    self.buckets[i + 1].insert(0, merged_bucket)

                except IndexError:
                    # if an IndexError occurs, it means there is no next bucket group
                    self.buckets.append([])
                    # create a new empty bucket group and insert the merged bucket 
                    # at the beginning
                    self.buckets[i + 1].insert(0, merged_bucket)

    def evaluate(self, end_ts):
        ones = 0
        last_bucket = 0

        for bucket_group in self.buckets:
            for bucket in bucket_group:
                # check if the final timestamp of the current bucket is less 
                # than the given end timestamp
                if bucket.final_ts < end_ts:
                    # exit the inner loop as we reached a bucket that is older 
                    # than the end timestamp
                    break 
                else:
                    ones += bucket.ones
                    last_bucket = bucket.ones

        # add half of the ones count in the last bucket 
        # (compensating for window size)
        ones += math.floor(last_bucket / 2)

        return ones


def quiet_logging(context):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def dgim(incoming_stream, prev_stream):
    samples = []
    queue = Queue()

    # resets every new stream
    timestamp = 0
    real_number_of_ones = 0

    for elem in incoming_stream:
        if elem == "1":
            real_number_of_ones += 1

            # create a new bucket with the current timestamp and add it to the queue
            queue.push(Bucket(timestamp, 1))

        timestamp += 1

    # evaluate the queue for the specified time window
    result = queue.evaluate(timestamp - WINDOW_SIZE) 

    # append the collected sample to the list
    samples.append((WINDOW_SIZE, result, real_number_of_ones))
    
    return samples

def get_ordered_counts(rdd):
    # map the two lists into a struct (value, weight)
    counts_dict = rdd.flatMap(lambda x: x[1])    

    return counts_dict


if __name__ == "__main__":

    sc = SparkContext()

    k = int(sys.argv[1]) # window size
    WINDOW_SIZE = k

    batch_interval = int(sys.argv[2]) # batch interval of user given seconds

    ssc = StreamingContext(sc, batch_interval)
    ssc.checkpoint("dgim")

    quiet_logging(sc)

    # Create a DStream by reading from a socket
    lines = ssc.socketTextStream("localhost", 9999)

    # split each line into pairs (timestamp, position)
    pairs = lines.map(lambda line: line)

    pre_sampled_data = pairs.map(lambda bit: (0, bit))

    # update the state of the DStream using dgim function
    sampled_data = pre_sampled_data.updateStateByKey(dgim)

    ordered_counts = sampled_data.transform(get_ordered_counts)

    print("\nResult: (window, number_of_ones)")
    ordered_counts.pprint(5)

    ssc.start()
    ssc.awaitTermination()