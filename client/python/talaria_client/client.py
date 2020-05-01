import grpc
from . import talaria_pb2_grpc
from . import talaria_pb2
from .encoder import Encoder

# TODO: Add Type Hints
class Client:

    def __init__(self, address):
        self.channel = grpc.insecure_channel(address)  # TODO: Enable Options
        self.ingress = talaria_pb2_grpc.IngressStub(self.channel)

    def ingest_batch(self, batch):
        encoder = Encoder()
        encoded = encoder.encode(batch)

        ingest_req = talaria_pb2.IngestRequest(batch=encoded)
        response = self.ingress.Ingest(ingest_req)

        return response
