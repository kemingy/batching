import logging
from pydantic import BaseModel
from ventu import Ventu


class Req(BaseModel):
    text: str


class Resp(BaseModel):
    label: bool


class ModelInference(Ventu):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def preprocess(self, data: Req):
        return data.text

    def batch_inference(self, data):
        print(data)
        return [True if len(text) >= 4 else False for text in data]

    def postprocess(self, data):
        return {'label': data}


if __name__ == "__main__":
    logger = logging.getLogger()
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    model = ModelInference(Req, Resp, use_msgpack=True)
    model.run_socket('batching.socket')
