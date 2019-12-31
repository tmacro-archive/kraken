import time

# Calls the decorated function repeatedly until duration secs have passed
def for_duration(duration, func, *args, **kwargs):
    start = time.time()
    while True:
        current = time.time()
        if current >= start + duration:
            break
        func(*args, **kwargs)

