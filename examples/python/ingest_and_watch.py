"""Upload a local UTF-8 or RTF file and inspect its change events over gRPC."""

import argparse
from pathlib import Path
import time

import grpc
from sanctum.v1 import sanctum_pb2 as pb, sanctum_pb2_grpc as rpc


def upload_frames(scope, path):
    yield pb.UploadTextFrame(header=pb.UploadTextHeader(scope=scope, name=path.name))
    with path.open("rb") as source:
        while data := source.read(256 * 1024):
            yield pb.UploadTextFrame(data=data)
    yield pb.UploadTextFrame(end=pb.UploadTextEnd())


def wait_until_ready(client, scope, operation_id):
    deadline = time.monotonic() + 30
    request = pb.IngestionJobRequest(scope=scope, operation_id=operation_id)
    while (remaining := deadline - time.monotonic()) > 0:
        job = client.GetIngestionJob(request, timeout=remaining)
        if job.state == pb.INGESTION_STATE_TEXT_READY:
            return job.text_revision
        if job.state in (pb.INGESTION_STATE_FAILED, pb.INGESTION_STATE_CANCELED):
            raise RuntimeError(job.error_message or pb.IngestionState.Name(job.state))
        time.sleep(0.1)
    raise TimeoutError(f"Check the status of operation {operation_id}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path)
    parser.add_argument("--endpoint", default="127.0.0.1:47550")
    parser.add_argument("--tenant", default="default")
    parser.add_argument("--collection", default="notes")
    parser.add_argument("--query")
    args = parser.parse_args()

    scope = pb.Scope(tenant=args.tenant, collection=args.collection)
    with grpc.insecure_channel(args.endpoint) as channel:
        client = rpc.SanctumStub(channel)
        try:
            client.CreateCollection(pb.CreateCollectionRequest(scope=scope), timeout=10)
        except grpc.RpcError as error:
            if error.code() != grpc.StatusCode.ALREADY_EXISTS:
                raise
        cursor = client.GetChangeCursor(pb.ChangeCursorRequest(scope=scope), timeout=10).cursor
        job = client.SubmitText(upload_frames(scope, args.file), timeout=30)
        revision = wait_until_ready(client, scope, job.operation_id)
        print(f"Indexed {revision.source_id} revision {revision.revision_id}")

        events = client.WatchChanges(pb.ListChangesRequest(scope=scope, after_cursor=cursor), timeout=10)
        try:
            for page in events:
                if any(change.revision_id == revision.revision_id for change in page.changes):
                    print(f"Observed publication at change cursor {page.next_cursor}")
                    break
        finally:
            events.cancel()

        if args.query:
            response = client.SearchText(pb.SearchTextRequest(scope=scope, query=args.query), timeout=10)
            for hit in response.hits:
                source_range = client.FetchSourceRange(pb.FetchSourceRangeRequest(
                    scope=hit.chunk.scope, source_id=hit.chunk.source_id,
                    revision_id=hit.chunk.revision_id, start_byte=hit.start_byte,
                    end_byte=hit.end_byte,
                ), timeout=10)
                print(f"{source_range.source_id}: {source_range.text}")


if __name__ == "__main__":
    main()
