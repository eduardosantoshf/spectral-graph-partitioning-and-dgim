import sys
import socket
import random
import sys
import time

if __name__ == "__main__":
    N = int(sys.argv[1]) # size of the generated stream
    # Initialization of the variables
    probability_1 = 0.2
    # Socket comms
    out_address = "localhost"
    out_port = 9999
    s = socket.socket()
    s.bind((out_address, out_port))
    s.listen(1)
    c, addr = s.accept()
    count = 1
    
    try:
        while True: 
            #time.sleep(1)
            random_n = random.uniform(0, 1)
            if random_n < probability_1:
                byte = '1\n'
            else:
                byte = '0\n'
            c.sendall(byte.encode('utf-8'))
            
            #print(f"byte #{count}: {byte}")
            
            count += 1

            if count > N:
                break

 # Catch the KeyboardInterrupt to stop stream generation gracefully
    except KeyboardInterrupt:
        print("Closing gracefully")
        # Close the socket for no problems
        c.close()
        s.close()
        sys.exit(1)

    c.close()
    s.close()