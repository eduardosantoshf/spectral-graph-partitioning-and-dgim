import sys
import socket
import time
from datetime import datetime, timedelta

if __name__ == "__main__":

    # Initialization of the variables
    interval = 1
    last_timestamp = ""
    # File to read
    in_filename = "../../../data/stream-data.csv"
    # Socket comms
    out_address = "localhost"
    out_port = 9999
    s = socket.socket()
    s.bind((out_address, out_port))
    s.listen(1)
    c, addr = s.accept()

    # Processing of the CSV
    input_f = open(in_filename,'rt')

    try:
        input_f.readline()
        while True:
            line = input_f.readline()

            if not line:
                break
            raw_timestamp, location = line.split(" ")[1].split(",")
            time_object = datetime.strptime(raw_timestamp, "%H:%M:%S.%f")
            timestamp = datetime.strftime(time_object,"%H:%M:%S")
            timestamp = datetime.strptime(timestamp, "%H:%M:%S")

            # check if last_timestamp is empty, if so, initialize it
            if last_timestamp == "":
                last_timestamp = timestamp

            # check if we reached the given timestamp
            stopping_timestamp = last_timestamp + timedelta(seconds=interval)
            if stopping_timestamp == timestamp:
                # close and lose access, so its updated in the Spark program
                print("Stopping!\n\n")
                time.sleep(interval)
                last_timestamp = stopping_timestamp

            print("Timestamp=%s \t\t Interval=%d \t\t StoppingTS=%s:%s:%s" % (raw_timestamp, interval, stopping_timestamp.hour, stopping_timestamp.minute, stopping_timestamp.second))
            c.sendall(line.encode('utf-8'))

    except Exception as e:
        print(e)
        print("Closing gracefully")

        input_f.close()
        s.close()
        sys.exit(1)

    input_f.close()
    s.close()