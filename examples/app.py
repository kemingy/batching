import logging
from pydantic import BaseModel
from ventu import Ventu


class Req(BaseModel):
    num: int


class Resp(BaseModel):
    square: int


class ModelInference(Ventu):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def preprocess(self, data: Req):
        return data.num

    def batch_inference(self, data):
        return [num ** 2 for num in data]

    def postprocess(self, data):
        return {'square': data}


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
