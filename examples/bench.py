from random import choice
from concurrent import futures
from time import time

import httpx
import msgpack


URL = 'http://localhost:8080/inference'
HEADER = {'Content-Type': 'application/msgpack'}
packer = msgpack.Packer(
    autoreset=True,
    use_bin_type=True,
)
TEXT = [
    'how do you like the weather here?',
    'The quick brown fox jumps over the lazy dog.',
    'An apple a day keeps the doctor away.',
    'Well begun is half done.',
    'A little learning is a dangerous thing',
]


def request(limit):
    t0 = time()
    with httpx.Client(headers=HEADER) as client:
        for _ in range(limit):
            resp = client.post(
                URL,
                data=packer.pack({'text': choice(TEXT)})
            )
            if resp.status_code != 200:
                print(resp.status_code, resp.content)
    return time() - t0


if __name__ == "__main__":
    worker_number = 8
    with futures.ThreadPoolExecutor(max_workers=worker_number) as executor:
        rounds = [1000] * worker_number
        results = list(executor.map(request, rounds))
        print(f'cost time: {list(results)}\ntotal: {sum(results)}')
