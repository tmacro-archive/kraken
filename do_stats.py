#!/usr/bin/python

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import PosixPath
from pprint import pprint

from dateutil.parser import isoparse as fromisoformat

def pathfile(opt):
    return PosixPath(opt).resolve()

def get_args():
    parser = argparse.ArgumentParser(
		prog=os.path.basename(sys.argv[0]),
		description='Survey the damage...')
    # parser.add_argument('-t', '--target', action='store', type=cloud_target, required=True, help='Specify the kraken\'s target. (blob/s3)')
    # parser.add_argument('-p', '--procs', action='store', type=int_or_none, default=None, help='Specify the number of processes to use when uploading.')
    # parser.add_argument('-s', '--size', action='store', type=int, default=1024, help='Specify the file size in bytes for put workloads.')
    # parser.add_argument('-d', '--duration', action='store', type=int, default=60, help='Load duration in seconds.')
    # parser.add_argument('-a', '--action', action='store', type=cloud_op, default='put', help='Operation to perform on the target. (put/get)')
    # parser.add_argument('-b', '--bucket', action='store', default='testbucket', help='Specify the bucket/container to use for workloads.')
    # parser.add_argument('-k', '--key-prefix', action='store', default='object', help='Specify the prefix to use for keys during uploads.')
    # parser.add_argument('-o', '--output', action='store', required=True, type=path, help='Specifiy the path to use for results data.')
    # parser.add_argument('--cs', '--connection-string', action='store', dest='connect_str', default=None, help='Specify the Azure connction string for blob targets. If not provided it is read from the env var `AZURE_STORAGE_CONNECTION_STRING`')
    parser.add_argument('-f', action='append', default=[], type=filepath, help='Specify samples to consume. Can be used multiple times')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Enable verbose output')
    return parser.parse_args()


def as_list(f):
    def inner(*args, **kwargs):
        return list(f(*args, **kwargs))
    return inner

def load_samples(path):
    with open(path) as samples:
        return json.load(samples)

def hydrate_samples(samples):
    for sample in samples:
        sample['start'] = math.floor((fromisoformat(sample['start']) - datetime.min.replace(tzinfo=timezone.utc)) / timedelta(milliseconds=1))
        sample['stop'] = math.floor((fromisoformat(sample['stop']) - datetime.min.replace(tzinfo=timezone.utc)) / timedelta(milliseconds=1))
        sample['duration'] = sample['stop'] - sample['start']


@as_list
def get_reads(samples):
    for op in samples:
        if op['type'] == 'get':
            yield op

@as_list
def get_writes(samples):
    for op in samples:
        if op['type'] == 'put':
            yield op

def get_start_ts(samples):
    ts = None
    for op in samples:
        if ts is None or op['start'] < ts:
            ts = op['start']
    return ts

def get_stop_ts(samples):
    ts = None
    for op in samples:
        if ts is None or op['stop'] > ts:
            ts = op['stop']
    return ts

def _find_next_slice(ts, slices):
    for s in slices:
        if ts < s:
            return s

def slice_samples(samples, length=10):
    slices = defaultdict(list)
    if not samples:
        return slices
    slice_start_ts = list(range(get_start_ts(samples), get_stop_ts(samples) + 20000, 10000))
    current_slice = 0
    for op in  sorted(samples, key=lambda op: op['start']):
        if op['start'] >= slice_start_ts[current_slice + 1]:
            current_slice = slice_start_ts.index(_find_next_slice(op['start'], slice_start_ts[current_slice:]))
        slices[slice_start_ts[current_slice]].append(op)
    return { ts: slices[ts] for ts in slice_start_ts if slices[ts]}

def _average_ms_slice(samples):
    total = sum(map(lambda op: op['duration'], samples))
    return total / len(samples)

def average_ms_slices(slices):
    avg = {}
    for ts, ops in slices.items():
        avg[ts] = _average_ms_slice(ops)
    return avg

def _avergage_ops_slice(samples, duration):
    return len(samples) / duration

def average_ops_slices(slices, duration):
    avg = {}
    for ts, ops in slices.items():
        avg[ts] = _avergage_ops_slice(ops, duration)
    return avg

def get_avg_ms_samples(samples):
    slices = slice_samples(samples)
    if not slices:
        return None
    slice_averages = average_ms_slices(slices)
    global_average = math.floor(sum(slice_averages.values()) / len(slice_averages.keys()))
    return global_average

def get_avg_ops_samples(samples):
    slices = slice_samples(samples)
    if not slices:
        return None
    slice_averages = average_ops_slices(slices, 10)
    global_average = math.floor(sum(slice_averages.values()) / len(slice_averages.keys()))
    return global_average

def get_success_percentage(samples):
    num_samples = len(samples)
    num_succeeded = len(list(filter(lambda s: s['result'], samples)))
    return num_succeeded / num_samples
       
def log_errors(samples):
    for s in samples:
        if s['error']:
            print(s['error'])

if __name__ == '__main__':
    for sample_path in sys.argv[1:]:
        print('~~ %s ~~'%sample_path)
        samples = load_samples(sample_path)
        hydrate_samples(samples)
        read_samples = get_reads(samples)
        write_samples = get_writes(samples)
        read_ms = get_avg_ms_samples(read_samples)
        read_ops = get_avg_ops_samples(read_samples)
        write_ms = get_avg_ms_samples(write_samples)
        write_ops = get_avg_ops_samples(write_samples)
        if read_ms or read_ops:
            print('READ:  %sms  %sops'%(read_ms, read_ops))
            success_per = get_success_percentage(read_samples)
            print('Success: %f%%'%success_per)
            if success_per < 1:
                log_errors(read_samples)
        if write_ms or write_ops:
            print('WRITE: %sms  %sops'%(write_ms, write_ops))
            success_per = get_success_percentage(write_samples)
            print('Success: %f%%'%success_per)
            if success_per < 1:
                log_errors(write_samples)