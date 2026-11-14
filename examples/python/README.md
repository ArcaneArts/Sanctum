# Sanctum gRPC client in Python

Start the [local gRPC server](../../docs/LOCAL_API.md) with its default
loopback listener. Run the following commands from the repository root.
Use a virtual environment for the generator and runtime.

```sh
python3 -m venv /tmp/sanctum-python-venv
. /tmp/sanctum-python-venv/bin/activate
python -m pip install grpcio-tools==1.73.0
generated="$(mktemp -d)"
python -m grpc_tools.protoc -I proto \
  --python_out="$generated" --grpc_python_out="$generated" \
  proto/sanctum/v1/sanctum.proto
PYTHONPATH="$generated" python examples/python/ingest_and_watch.py README.md \
  --collection python-example --query Sanctum
```

The example generates the client from the same contract used by Rust.
It creates a collection if needed, streams the file into the durable
queue, waits for lexical publication, reads its change event, and
searches the collection. Run it against a dedicated collection since it
creates a new source on each run. [gRPC Python quickstart](https://grpc.io/docs/languages/python/quickstart/).

## Read structured errors

Install `grpcio-status==1.73.0` into the same environment. Unpack Sanctum's
detail with the generated Protobuf class:

```python
from grpc_status import rpc_status
from sanctum.v1 import sanctum_pb2 as pb


def sanctum_error(error):
    envelope = rpc_status.from_call(error)
    if envelope is not None:
        for payload in envelope.details:
            detail = pb.RpcError()
            if payload.Unpack(detail):
                return detail
    return None
```

For an `UploadText` error with reason `upload_pending`, use the returned
operation ID to check `GetIngestionJob` in the original scope.
[Error details contract](../../docs/LOCAL_API.md#grpc-error-details).
