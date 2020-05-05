from concurrent import futures
import httpx
import msgpack


URL = 'http://localhost:8080'
packer = msgpack.Packer(
    autoreset=True,
    use_bin_type=True,
)


def request(text):
    with httpx.Client() as client:
        resp = client.post(URL, data=packer.pack({'text': text}))
        if resp.status_code == 200:
            print(text, msgpack.unpackb(resp.content, raw=False))


if __name__ == "__main__":
    with futures.ThreadPoolExecutor() as executor:
        list(executor.map(request, ('hello world', 'test', 'hi')))
