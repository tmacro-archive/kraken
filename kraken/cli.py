import argparse
import os
import sys
import pathlib

def cloud_target(opt):
    if opt not in ['blob', 's3']:
        raise ValueError('%s is not one of blob, s3'%opt)
    return opt

def cloud_op(opt):
    if opt not in ['put', 'get']:
        raise ValueError('%s is not one of put, get'%opt)
    return opt

def int_or_none(opt):
    if opt is None:
        return opt
    return int(opt)

def path(opt):
    return pathlib.PosixPath(opt).resolve()

def get_args():
    parser = argparse.ArgumentParser(
		prog=os.path.basename(sys.argv[0]),
		description='Summon the kraken!')
    parser.add_argument('-t', '--target', action='store', type=cloud_target, required=True, help='Specify the kraken\'s target. (blob/s3)')
    parser.add_argument('-p', '--procs', action='store', type=int_or_none, default=None, help='Specify the number of processes to use when uploading.')
    parser.add_argument('-s', '--size', action='store', type=int, default=1024, help='Specify the file size in bytes for put workloads.')
    parser.add_argument('-d', '--duration', action='store', type=int, default=60, help='Load duration in seconds.')
    parser.add_argument('-a', '--action', action='store', type=cloud_op, default='put', help='Operation to perform on the target. (put/get)')
    parser.add_argument('-b', '--bucket', action='store', default='testbucket', help='Specify the bucket/container to use for workloads.')
    parser.add_argument('-k', '--key-prefix', action='store', default='object', help='Specify the prefix to use for keys during uploads.')
    parser.add_argument('-o', '--output', action='store', required=True, type=path, help='Specifiy the path to use for results data.')
    parser.add_argument('--cs', '--connection-string', action='store', dest='connect_str', default=None, help='Specify the Azure connction string for blob targets. If not provided it is read from the env var `AZURE_STORAGE_CONNECTION_STRING`')
    parser.add_argument('--ak', '--access-key', action='store', dest='access_key', default=None, help='Specify the Access Key to use for s3 targets. If not provided it is read from the env var `ACCESS_KEY`')
    parser.add_argument('--sk', '--secret-key', action='store', dest='secret_key', default=None, help='Specify the Secret Key to use for s3 targets. If not provided it is read from the env var `SECRET_KEY`')
    parser.add_argument('--s3-endpoint', action='store', default=None, help='Specify the s3 endpoint to use. If not provided `s3.amazon.com` is used.')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Enable verbose logging')
    return parser.parse_args()