from . import pool
from .blob import BlobDriver, BlobDriverConfig
from .cli import get_args
# from .driver import DriverConfig
from .utils import for_duration
import os
from collections import namedtuple

DriverConfig = namedtuple('DriverConfig', ['cls', 'config'])
WorkloadConfig = namedtuple('WorkloadConfig', ['driver', 'action', 'output', 'duration', 'obj_size', 'bucket', 'key_prefix', 'key_start', 'key_step'], defaults=[None, None])

def build_blob_driver_config(args):
    if args.connect_str:
        return BlobDriver, BlobDriverConfig(args.connect_str)
    elif os.environ.get('AZURE_STORAGE_CONNECTION_STRING') is not None:
        return BlobDriver, BlobDriverConfig(os.environ.get('AZURE_STORAGE_CONNECTION_STRING'))
    raise Exception('You must provide either the `--connection-string` flag or set the `AZURE_STORAGE_CONNECTION_STRING` environment variable!')

def build_s3_driver_config(args):
    raise NotImplementedError('The s3 driver has not been implemented')

def build_driver_config(args):
    if args.target == 'blob':
        return DriverConfig(*build_blob_driver_config(args))
    elif args.target == 's3':
        return DriverConfig(*build_s3_driver_config(args))

def build_workload_config(args):
    return WorkloadConfig(
        driver=build_driver_config(args),
        action=args.action,
        output=args.output,
        duration=args.duration,
        obj_size=args.size,
        bucket=args.bucket,
        key_prefix=args.key_prefix)

def entry():
    args = get_args()
    workload_conf = build_workload_config(args)
    pool.execute(workload_conf, args.procs, verbose=args.verbose)
