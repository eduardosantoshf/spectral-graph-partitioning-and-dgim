# Exponentially Decaying Window

## How to run 

1. Run the stream script:

```
python3 stream.py
```

2. Run:

```
spark-submit exponentially_decaying_window.py <user_defined_interval>
```

example:

```
spark-submit exponentially_decaying_window.py 5
```

The results will be printed as:

(event, count)