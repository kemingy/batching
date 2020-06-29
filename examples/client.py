from concurrent import futures
import httpx
import msgpack


URL = 'http://localhost:8080/inference'
HEADER = {'Content-Type': 'application/msgpack'}
packer = msgpack.Packer(
    autoreset=True,
    use_bin_type=True,
)


def request(text):
    return httpx.post(URL, data=packer.pack({'text': text}), headers=HEADER)


if __name__ == "__main__":
    with futures.ThreadPoolExecutor() as executor:
        text = [
            'They are smart',
            'what is your problem?',
            'I hate that!',
            'x',
        ]
        results = executor.map(request, text)
        for i, resp in enumerate(results):
            print(
                f'>> {text[i]} -> [{resp.status_code}]\n'
                f'{msgpack.unpackb(resp.content)}'
            )
