from . import pool
from .blob import BlobDriver, BlobDriverConfig
from .cli import get_args
from .driver import DriverConfig
from .utils import for_duration
import os


def do_put(driver, config):
    return for_duration(config.duration, driver.put, config.obj_size)
    
def do_get(driver, config):
    driver.get()

def build_blob_driver_config(args):
    if args.connect_str:
        return BlobDriverConfig(args.connect_str)
    elif os.environ.get('AZURE_STORAGE_CONNECTION_STRING') is not None:
        return BlobDriverConfig(os.environ.get('AZURE_STORAGE_CONNECTION_STRING'))
    raise Exception('You must provide either the `--connection-string` flag or set the `AZURE_STORAGE_CONNECTION_STRING` environment variable!')

def build_driver_config(args):
    if args.target == 'blob':
        driver = BlobDriver
        driver_conf = build_blob_driver_config(args)
    if args.action == 'put':
        action = do_put
    return DriverConfig(driver, driver_conf, action, args.duration, args.size)

def entry():
    args = get_args()
    config = build_driver_config(args)
    pool.execute(config, args.procs)
