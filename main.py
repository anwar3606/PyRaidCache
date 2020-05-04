import hashlib
import os
import pickle
import threading
import time
from functools import wraps

RAID_DISKS = [
    'J:',
    'K:'
]


def _get_next_disk_generator():
    total_disks = len(RAID_DISKS)
    next_disk_index = 0
    while True:
        yield RAID_DISKS[next_disk_index]
        next_disk_index += 1
        if next_disk_index > total_disks - 1:
            next_disk_index = 0


_next_disk = _get_next_disk_generator()


def get_next_disk():
    return next(_next_disk)


def lookup_hash_file(hash_file):
    return os.path.exists(hash_file)


def get_hash_file_path(disk, url):
    lookup_hash = hashlib.md5(url.encode()).hexdigest()
    return disk + os.path.sep + lookup_hash + '.pickle'


def load_hash_file(url):
    disk = get_next_disk()

    hash_file = get_hash_file_path(disk, url)
    if lookup_hash_file(hash_file):
        return pickle.load(open(hash_file, 'rb'))


def save_hash_file(url, response):
    for disk in RAID_DISKS:
        hash_file = get_hash_file_path(disk, url)

        pickle.dump(response, open(hash_file, 'wb'))


def cache_response(f):
    @wraps(f)
    def wrapper(url):
        old_response = load_hash_file(url)
        if old_response:
            return old_response

        response = f(url)

        threading.Thread(target=save_hash_file, args=(url, response), daemon=True).start()

        return response

    return wrapper


@cache_response
def test_func(url):
    data = {}
    for i in range(1000):
        data["key{}".format(i)] = "{0}{0}{0}{0}{0}".format(i)
    time.sleep(.05)
    return data


def test_func_without_cache(url):
    data = {}
    for i in range(1000):
        data["key{}".format(i)] = "{0}{0}{0}{0}{0}".format(i)
    time.sleep(.05)
    return data


if __name__ == '__main__':
    from timeit import default_timer as timer

    start = timer()
    for i in range(100):
        test_func("testall{}".format(i))
    end = timer()
    print("Time with cache: {}".format(end - start))

    start = timer()
    for i in range(100):
        test_func_without_cache("testall{}".format(i))
    end = timer()
    print("Time without cache: {}".format(end - start))
