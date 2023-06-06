from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys

C = 10 ** -6 # decay factor

def quiet_logging(context):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def decaying_window(incoming_stream, previous_stream):
    # we add the whole stream as an empty array and a count of 0 elements
    if previous_stream == None:
        previous_stream = [[], []]

    # get the distinct elements in the incoming stream
    distinct_elems = set(incoming_stream)

    sample_values = []
    sample_weights = []

    # iterate every new user on the stream, calculate weights for each one
    for elem in distinct_elems:
        count = 1 if incoming_stream[0] == elem else 0

        # calculate the weight for the current element in the stream
        for inc_elem_idx in range(len(incoming_stream)):
            if inc_elem_idx == 0: 
                # apply the decay factor for the first element
                count = count * (1 - C) 

                continue
            
            bit = 1 if elem == incoming_stream[inc_elem_idx] else 0

            # calculate the weighted count using the decay factor and the matching bit
            count = count * (1 - C) + bit

        # add the element and its corresponding weight to the sample lists
        if elem not in sample_values:
            sample_values.append(elem)
            sample_weights.append(round(count))
        else:
            idx = sample_values.index(elem)
            # update the weight if the element already exists in the samples
            sample_weights[idx] += round(count)
    
    return [sample_values, sample_weights]

def get_ordered_counts(rdd):
    # map the two lists into a better struct: (value, weight)
    counts_dict = rdd.map(lambda x: x[1]) \
            .map(lambda x: [(x[0][i], x[1][i]) for i in range(len(x[0]))]) \
            .flatMap(lambda x: x)
    
    ordered_dict = counts_dict.sortBy(lambda x: x[1], ascending = False)

    return ordered_dict

if __name__ == "__main__":
    sc = SparkContext(appName = "ExponentiallyDecayingWindow")

    batch_interval = int(sys.argv[1]) # batch interval of user given seconds

    ssc = StreamingContext(sc, batch_interval)
    ssc.checkpoint("events")

    quiet_logging(sc)

    # Create a DStream by reading from a socket
    lines = ssc.socketTextStream("localhost", 9999)

    # split each line into pairs (timestamp, event)
    pairs = lines.map(lambda line: line.split(" ")[1].split(","))

    # map each pair to (0, event) for initial state 
    pre_sampled_data = pairs.map(lambda mention: (0, mention[1])) \

    # update the state of the DStream using decaying_window function
    sampled_data = pre_sampled_data.updateStateByKey(decaying_window)

    ordered_counts = sampled_data.transform(get_ordered_counts)

    ordered_counts.pprint(5) # print the 5 most frequent events

    ssc.start()
    ssc.awaitTermination()