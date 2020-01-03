import time
from collections import namedtuple
from datetime import datetime, timezone

# Calls the decorated function repeatedly until duration secs have passed
def for_duration(duration, func, *args, **kwargs):
    start = time.time()
    while True:
        current = time.time()
        if current >= start + duration:
            break
        yield func(*args, **kwargs)


InstrumentedCall = namedtuple('InstrumentedCall', ['start', 'stop', 'result'])

def instrument_call(func, *args, **kwargs):
    started = datetime.now(timezone.utc)
    try:
        result = func(*args, **kwargs)
    except Exception:
        result = False
    stopped = datetime.now(timezone.utc)
    return InstrumentedCall(started.isoformat(), stopped.isoformat(), not not result)

class Timer:
    def __init__(self, duration):
        self._started = None
        self._duration = duration

    def __call__(self):
        if self._started is None:
            self._started = time.time()
        elif time.time() - self._started >= self._duration:
            return False
        return True