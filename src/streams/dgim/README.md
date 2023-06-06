# DGIM

## How to run 

1. Run the stream generator script:

```
python3 generate_stream.py <number_of_bits_to_be_generated>
```

example:

```
python3 generate_stream.py 10000000
```

2. Run:

```
spark-submit dgim.py <window_size> <user_defined_interval>
```

example:

```
spark-submit dgim.py 10 10
```

The results will be printed as:

(window_size, estimated_number_of_1s)