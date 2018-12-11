#!/usr/bin/env python

__all__ = [
    'fetch',
    'switch_logging',
]

import atexit
import requests

import aiohttp
import asyncio
import async_timeout

import os
import sys
import json
import email.utils

from datetime import datetime, timedelta
from collections import namedtuple

now = datetime.now()
past = None
endpoint = 'https://api.treasuredata.com'
td_api_key = None
reqheaders = None

DEFAULT_PAGESIZE = 200
DEFAULT_CONCURRENCY = 5
DEFAULT_LIMIT = 1000
DEFAULT_INTERVAL_DAYS = 30
DEFAULT_TIMEOUT_SEC = 180

IO_RESOURCES = namedtuple('IO_RESOURCES', ('stdin', 'stdout', 'stderr', 'devnull'))
io = IO_RESOURCES(None, None, sys.stderr, open(os.devnull, 'w'))

sweep_file_obj = lambda f: f.close()
atexit.register(sweep_file_obj, io.devnull)

printdest = io.devnull


def fetch_users_info():
    url = '/'.join([endpoint, 'v3/user/list'])
    printdest.write('SEND REQUEST: {}\n'.format(url))
    printdest.flush()

    res = requests.get(url, headers=reqheaders)
    res.raise_for_status()
    return json.loads(res.text)


def analyze_jobs(joblist):

    def to_datetime(job, attr='created_at'):
        text = job.get(attr, now.strftime('%Y-%m-%d %H:%M:%S UTC'))
        return datetime.strptime(text, '%Y-%m-%d %H:%M:%S UTC')

    reduced_jobs = list(filter(lambda x: to_datetime(x) >= past, joblist.get('jobs')))
    unique_users = set(map(lambda x: x.get('user_name', None), reduced_jobs))
    unique_users = unique_users - { None }
    return (len(reduced_jobs), unique_users)


async def fetch_job_history(session, payload):
    url = '/'.join([endpoint, 'v3/job/list'])
    printdest.write('SEND REQUEST: {}?{}\n'.format(url, '&'.join([ '{}={}'.format(k, v) for k, v in payload.items() ])))
    printdest.flush()

    async with async_timeout.timeout(DEFAULT_TIMEOUT_SEC):
        async with session.get(url, params=payload, headers=reqheaders) as res:
            return json.loads(await res.text())


async def fetch_active_users(pagesize=200, concurrency=5, limit=100000):
    async with aiohttp.ClientSession() as session:
        payloads = [ {'from': i, 'to': i+pagesize-1} for i in range(0, limit, pagesize) ]

        active_users = set()
        while len(payloads) > 0:
            ready = payloads[:concurrency]

            futures = [ fetch_job_history(session, payload) for payload in ready ]
            returns = await asyncio.gather(*futures)
            payloads = payloads[concurrency:]

            inspects = list(map(analyze_jobs, returns))
            unique_users = set().union(*[ inspect[1] for inspect in inspects ])
            active_users = active_users | unique_users

            if not all(map(lambda x: x[0] == pagesize, inspects)):
                break

        return active_users


def prepare_variables(**kwargs):
    global printdest, past, td_api_key, reqheaders

    past = now - timedelta(days=kwargs.get('intervaldays', DEFAULT_INTERVAL_DAYS))
    td_api_key = kwargs.get('apikey', os.getenv('TD_API_KEY'))
    if td_api_key is None:
        raise('must be set either TD_API_KEY environ variable or apikey argument')

    reqheaders = {
        'accept-encoding': 'deflate, gzip',
        'authorization': 'TD1 {}'.format(td_api_key),
        'date': email.utils.formatdate(now.timestamp()),
        'user-agent': 'TD-Client-Python/0.12.1.dev0',
    }


def switch_logging(verbose):
    global printdest

    if verbose:
        printdest = io.stderr

    else:
        printdest = io.devnull


def fetch(**kwargs):
    '''
    Available Arguments
      - apikey (str):         authentication key for access to treasure-data. (for lib)
      - intervaldays (int):   period of job history.
      - pagesize (int):       entries per page.
      - concurrency (int):    async i/o concurrency.
      - limit (int):          max entries of requests.
    '''
    prepare_variables(**kwargs)

    users_info = fetch_users_info()

    necessary_keys = ('pagesize', 'concurrency', 'limit')
    subargs = dict(filter(lambda x: x[0] in necessary_keys, kwargs.items()))

    loop = asyncio.get_event_loop()
    active_users = loop.run_until_complete(fetch_active_users(**subargs))

    if kwargs.get('inverse', False):
        # return inactive user list
        return list(filter(lambda x: x.get('name') not in active_users, users_info.get('users')))

    else:
        # return active user list
        return list(filter(lambda x: x.get('name') in active_users, users_info.get('users')))


if __name__ == '__main__':
    import argparse
    from pprint import pprint

    parser = argparse.ArgumentParser(
                 prog=os.path.basename(__file__),
                 description='Listing up active/inactive users from treasure-data'
             )

    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='print progress log.')
    parser.add_argument('-r', '--inverse', action='store_true', default=False, help='listing up inactive users.')
    parser.add_argument('-i', '--interval-days', type=int, default=30, help='threshold that active or inactive.')
    parser.add_argument('-p', '--page-size', type=int, default=200, help='number of entries per api request.')
    parser.add_argument('-l', '--limit', type=int, default=300000, help='max entries that api requests.')
    parser.add_argument('-c', '--concurrency', type=int, default=5, help='number of concurrent run.')
    args = parser.parse_args()

    kwargs = {
        'intervaldays': args.interval_days,
        'pagesize': args.page_size,
        'limit': args.limit,
        'concurrency': args.concurrency,
        'inverse': args.inverse,
    }

    switch_logging(args.verbose)
    userlist = fetch(**kwargs)

    print(json.dumps(userlist))
    printdest.write('result size: {}\n'.format(len(userlist)))
