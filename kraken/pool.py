import multiprocessing as mp
from multiprocessing import Barrier, Process 
import datetime

def do_work(start, done, config):
    driver = config.driver()
    driver.setup(config.driver_config)
    start.wait()
    config.action(driver, config)
    done.wait()

def start_barrier_broken(duration):
    def inner():
        print('Start barrier has been broken, starting test of duration %s secs.'%duration)
        print('Started:\t%s'%datetime.datetime.now())
        print('Should finish:\t%s'%(datetime.datetime.now() + datetime.timedelta(seconds=duration)))
    return inner

def done_barrier_broken():
    print('Done barrier has been broken, test has finished.')

def execute(task_config,  num_procs=None):
    if num_procs is None:
        num_procs = mp.cpu_count()
    start = mp.Barrier(num_procs, action=start_barrier_broken(task_config.duration))
    done = mp.Barrier(num_procs + 1, action=done_barrier_broken) # Add one for controlling proc
    procs = [mp.Process(target=do_work, args=(start, done, task_config)) for x in range(num_procs)]
    for p in procs:
        p.start()
    done.wait()
    for p in procs:
        p.join()


    