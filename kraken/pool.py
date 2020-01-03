import multiprocessing as mp
from multiprocessing import Barrier, Process 
import datetime
import threading
import queue
from .utils import for_duration
import traceback
import json

def do_work(start, done, results, config):
    try:
        driver = config.driver.cls()
        driver.setup(config)
        start.wait()
        action = getattr(driver, config.action)
        for result in action():
            results.put(result)
        done.wait()
    except Exception as e:
        print('Exception in worker process')
        print(traceback.format_exc())
        done.wait()

def start_barrier_broken(duration):
    def inner():
        print('Start barrier has been broken, starting test of duration %s secs.'%duration)
        print('Started:\t%s'%datetime.datetime.now())
        print('Should finish:\t%s'%(datetime.datetime.now() + datetime.timedelta(seconds=duration)))
    return inner

def done_barrier_broken():
    print('Done barrier has been broken, test has finished.')

def result_writer(results, exit, done, output_path):
    with open(output_path, 'w') as output:
        output.write('[\n')
        while not exit.is_set() or not results.empty():
            try:
                result = results.get(timeout=5)
                print(result)
                output.write('%s,\n'%json.dumps(result._asdict()))
            except queue.Empty:
                pass
        # Strip off the last `,\n` and replace with `\n]`
        output.seek(output.tell() - 2)
        output.write('\n]')
    done.set()
    
def execute(workload_config,  num_procs=None):
    if num_procs is None:
        num_procs = mp.cpu_count()
        print('No worker count provided defaulting to number of CPUs: %d'%mp.cpu_count())
    # Setup the worker processes
    start = mp.Barrier(num_procs, action=start_barrier_broken(workload_config.duration))
    done = mp.Barrier(num_procs + 1, action=done_barrier_broken) # Add one for controlling proc
    results = mp.Queue()
    procs = []
    for i in range(num_procs):
        proc_workload_config = workload_config._replace(key_start=i, key_step=num_procs)
        proc = mp.Process(target=do_work, args=(start, done, results, proc_workload_config))
        proc.start()
        procs.append(proc)
    # Setup the results writer
    writer_exit = mp.Event()
    writer_done = mp.Event()
    writer_proc = mp.Process(target=result_writer, args=(results, writer_exit, writer_done, workload_config.output))
    writer_proc.start()
    # Wait for the workers to finish
    done.wait()
    # Do cleanup
    for p in procs:
        p.join()
    writer_exit.set()
    writer_done.wait()
    writer_proc.join()
    