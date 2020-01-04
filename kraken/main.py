from . import pool
from .driver import BlobDriver, BlobDriverConfig, S3Driver, S3DriverConfig
from .cli import get_args
# from .driver import DriverConfig
from .utils import for_duration
import os
from collections import namedtuple

DriverConfig = namedtuple('DriverConfig', ['cls', 'config'])
_WorkloadConfig = namedtuple('WorkloadConfig', ['driver', 'action', 'output', 'duration', 'obj_size', 'bucket', 'key_prefix', 'key_start', 'key_step'])
def WorkloadConfig(driver, action, output, duration, obj_size, bucket, key_prefix, key_start=None, key_step=None):
    return _WorkloadConfig(driver, action, output, duration, obj_size, bucket, key_prefix, key_start, key_step)

def build_blob_driver_config(args):
    if args.connect_str:
        return BlobDriver, BlobDriverConfig(args.connect_str)
    elif os.environ.get('AZURE_STORAGE_CONNECTION_STRING') is not None:
        return BlobDriver, BlobDriverConfig(os.environ.get('AZURE_STORAGE_CONNECTION_STRING'))
    raise Exception('You must provide either the `--connection-string` flag or set the `AZURE_STORAGE_CONNECTION_STRING` environment variable!')

def build_s3_driver_config(args):
    if args.access_key:
        access_key = args.access_key
    elif os.environ.get('ACCESS_KEY') is not None:
        access_key = os.environ.get('ACCESS_KEY')
    else:
        raise Exception('You must provide either the `--access-key` flag or set the `ACCESS_KEY` environment variable!')
    if args.secret_key:
        secret_key = args.secret_key
    elif os.environ.get('SECRET_KEY') is not None:
        secret_key = os.environ.get('SECRET_KEY')
    else:
        raise Exception('You must provide either the `--secret-key` flag or set the `SECRET_KEY` environment variable!')
    if args.s3_endpoint:
        s3_endpoint = args.s3_endpoint
    else:
        s3_endpoint = 'https://s3.amazonaws.com'
    return S3Driver, S3DriverConfig(access_key, secret_key, s3_endpoint)

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
