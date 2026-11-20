#!/usr/bin/env python3
"""Loopback TCP access to a fixture's internal Docker network, without an outbound route."""
import asyncio
from contextlib import suppress
import json
from pathlib import Path
import sys


async def main():
    # The ownership token in argv lets the operator verify a saved PID before signalling it.
    path, _owner = sys.argv[1:]
    targets = json.loads(Path(path).read_text())

    async def forward(reader, writer, host, port):
        upstream_writer = None
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(host, port)

            async def copy(source, destination):
                while chunk := await source.read(65536):
                    destination.write(chunk)
                    await destination.drain()
                with suppress(OSError):
                    destination.write_eof()

            await asyncio.gather(copy(reader, upstream_writer), copy(upstream_reader, writer))
        except OSError:
            pass
        finally:
            writer.close()
            if upstream_writer:
                upstream_writer.close()

    servers = []
    for local, host, port in targets:
        servers.append(await asyncio.start_server(
            lambda r, w, h=host, p=port: forward(r, w, h, p), '127.0.0.1', local))
    await asyncio.gather(*(server.serve_forever() for server in servers))


if __name__ == '__main__':
    asyncio.run(main())
