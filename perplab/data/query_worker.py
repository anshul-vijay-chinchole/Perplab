"""Heavy API SQL runs in a governed research process; response size is bounded."""
from __future__ import annotations
import json
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any
from perplab.resources import ResourceGuard, ResourceLimitExceeded, cache_reservation, worker_exit_reason

MAX_RESPONSE_BYTES = 256 * 1024 ** 2


class _LimitedOutput:
    def __init__(self, file, limit):
        self.file, self.limit = file, limit

    @property
    def closed(self):
        return self.file.closed

    def writable(self):
        return True

    def write(self, data):
        if self.file.tell() + len(data) > self.limit:
            raise ResourceLimitExceeded("query_response_limit: request a narrower range (response exceeds 256 MiB)")
        return self.file.write(data)

    def tell(self):
        return self.file.tell()

    def flush(self):
        return self.file.flush()


def remote_query(policy_root, market, sql, datasets, params):
    import pyarrow as pa
    parent = Path(policy_root) / "_queries"
    directory = parent / uuid.uuid4().hex
    process = None
    with cache_reservation(Path(market), MAX_RESPONSE_BYTES):
        directory.mkdir(parents=True)
        try:
            process = subprocess.Popen([sys.executable, "-m", __name__, str(policy_root), str(directory)],
                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            _, error = process.communicate(json.dumps({"market": str(market), "sql": sql,
                "datasets": list(datasets) if datasets is not None else None, "params": params}).encode(), timeout=300)
            if process.returncode:
                detail = error.decode("utf-8", "replace")[-2000:].strip() or worker_exit_reason(directory, process.returncode)
                raise ResourceLimitExceeded("query worker failed: " + detail)
            # Bounded copy allows Windows to delete the IPC file immediately; no
            # mapped file handle is retained in a server response.
            path = directory / "result.arrow"
            if path.stat().st_size > MAX_RESPONSE_BYTES:
                raise ResourceLimitExceeded("query_response_limit: request a narrower range")
            blob = path.read_bytes()
            return pa.ipc.open_stream(pa.BufferReader(blob)).read_all()
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            raise ResourceLimitExceeded("timeout: query waited or ran for more than five minutes") from None
        finally:
            if process is not None and process.poll() is None:
                process.kill()
                process.wait()
            target, boundary = directory.resolve(), parent.resolve()
            if not target.is_relative_to(boundary) or target == boundary:
                raise RuntimeError("query cleanup escaped its temporary directory")
            shutil.rmtree(target)


def main() -> int:
    root, directory = map(Path, sys.argv[1:])
    try:
        with ResourceGuard(root, "query", directory=directory):
            import pyarrow as pa
            from perplab.data.query import stream_query
            request = json.loads(sys.stdin.buffer.read())
            path = directory / "result.arrow"
            with path.open("wb") as file:
                output = _LimitedOutput(file, MAX_RESPONSE_BYTES)
                writer = None
                try:
                    for batch in stream_query(request["market"], request["sql"],
                            datasets=request["datasets"], params=request["params"]):
                        if writer is None:
                            writer = pa.ipc.new_stream(output, batch.schema)
                        writer.write_batch(batch)
                        if output.tell() > MAX_RESPONSE_BYTES:
                            raise ResourceLimitExceeded("query_response_limit: request a narrower range (response exceeds 256 MiB)")
                    if writer is None:
                        # The reader yields an empty batch with a schema on current
                        # DuckDB; preserve a schema even on versions that do not.
                        from perplab.data.query import connect
                        con = connect(request["market"], datasets=request["datasets"])
                        try:
                            cursor = con.execute(request["sql"], request["params"])
                            reader = cursor.to_arrow_reader(1)
                            writer = pa.ipc.new_stream(output, reader.schema)
                        finally:
                            con.close()
                finally:
                    if writer:
                        writer.close()
    except BaseException as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
