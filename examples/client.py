from concurrent import futures
import httpx
import msgpack


URL = 'http://localhost:8080'
packer = msgpack.Packer(
    autoreset=True,
    use_bin_type=True,
)


def request(text):
    return httpx.post(URL, data=packer.pack({'num': text}))


if __name__ == "__main__":
    with futures.ThreadPoolExecutor() as executor:
        text = (0, 'test', -1, 233)
        results = executor.map(request, text)
        for i, resp in enumerate(results):
            print(
                f'>> {text[i]} -> [{resp.status_code}]\n'
                f'{msgpack.unpackb(resp.content, raw=False)}'
            )
