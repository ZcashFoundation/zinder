"""Read-only loopback gRPC adapter for byte-level wallet fixture experiments."""
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import grpc
import service_pb2


SERVICE = service_pb2.DESCRIPTOR.services_by_name["CompactTxStreamer"]
READ_METHODS = {method.name: method for method in SERVICE.methods
                if not method.client_streaming and method.name not in {"SendTransaction", "Ping"}}


class RawReader:
    def __init__(self, channel, timeout=120):
        self.channel = channel
        self.timeout = timeout

    def read(self, method_name, request_bytes):
        method = READ_METHODS[method_name]
        path = f"/{SERVICE.full_name}/{method_name}"
        if method.server_streaming:
            return self.channel.unary_stream(path)(request_bytes, timeout=self.timeout)
        return iter((self.channel.unary_unary(path)(request_bytes, timeout=self.timeout),))


def serve(handler, port=0):
    """Start a private listener. handler(name, request_bytes, context) yields raw messages."""
    def stream(name, request, context):
        try:
            yield from handler(name, request, context)
        except grpc.RpcError as error:
            context.abort(error.code(), error.details())

    def unary(name, request, context):
        response, = stream(name, request, context)
        return response

    methods = {}
    for method in READ_METHODS.values():
        if method.server_streaming:
            methods[method.name] = grpc.unary_stream_rpc_method_handler(partial(stream, method.name))
        else:
            methods[method.name] = grpc.unary_unary_rpc_method_handler(partial(unary, method.name))
    server = grpc.server(ThreadPoolExecutor(max_workers=24), options=(("grpc.so_reuseport", 0),))
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(SERVICE.full_name, methods),))
    bound = server.add_insecure_port(f"127.0.0.1:{port}")
    if bound == 0:
        raise RuntimeError("could not bind loopback gRPC listener")
    server.start()
    return server, bound
