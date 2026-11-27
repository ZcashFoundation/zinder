#!/usr/bin/env python3
"""Loopback TCP access to a fixture's internal Docker network, without an outbound route."""
import asyncio
from contextlib import suppress
import hmac
import json
from pathlib import Path
import secrets
import signal
import sys

MAX_EXEC_STREAMS = 64

# Docker Desktop cannot route from the host to an internal bridge. Run the TCP
# stream inside the pinned node image, with Docker carrying stdin and stdout.
EXEC_TCP = r'''
use IO::Socket::INET;
use POSIX qw(_exit);
my $socket = IO::Socket::INET->new(PeerHost => $ARGV[0], PeerPort => $ARGV[1], Timeout => 5)
    or die "connect fixture endpoint: $!";
sub copy_stream {
    my ($from, $to) = @_;
    while (my $size = sysread($from, my $bytes, 65536)) {
        my $offset = 0;
        while ($offset < $size) {
            my $written = syswrite($to, $bytes, $size - $offset, $offset) or return;
            $offset += $written;
        }
    }
}
my $upload = fork();
defined($upload) or die "fork fixture TCP stream: $!";
if (!$upload) {
    copy_stream(\*STDIN, $socket);
    shutdown($socket, 1);
    _exit(0);
}
$SIG{PIPE} = 'IGNORE';
copy_stream($socket, \*STDOUT);
kill 'TERM', $upload;
waitpid($upload, 0);
'''


async def copy_stream(source, destination):
    while chunk := await source.read(65536):
        destination.write(chunk)
        await destination.drain()
    with suppress(OSError):
        destination.write_eof()


async def forward(reader, writer, host, port, exec_container):
    process = None
    upstream_writer = None
    streams = []
    try:
        if exec_container:
            process = await asyncio.create_subprocess_exec(
                'docker', 'exec', '-i', exec_container, 'perl', '-e', EXEC_TCP, host, str(port),
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
            upstream_reader, upstream_writer = process.stdout, process.stdin
        else:
            upstream_reader, upstream_writer = await asyncio.open_connection(host, port)
        streams = [
            asyncio.create_task(copy_stream(reader, upstream_writer)),
            asyncio.create_task(copy_stream(upstream_reader, writer)),
        ]
        await asyncio.gather(*streams)
    except OSError:
        pass
    finally:
        writer.close()
        for stream in streams:
            stream.cancel()
        await asyncio.gather(*streams, return_exceptions=True)
        if upstream_writer:
            upstream_writer.close()
        if process:
            with suppress(ProcessLookupError):
                process.kill()
            await process.communicate()


async def main():
    # The ownership token in argv lets the operator verify a saved PID before signalling it.
    path, _owner = sys.argv[1:]
    routes = json.loads(Path(path).read_text())
    managed = isinstance(routes, dict)
    targets = routes['targets'] if managed else routes
    exec_container = routes['exec_container'] if managed else None
    active = set()
    stopping = asyncio.Event()
    stopped = asyncio.Event()
    reply_sent = asyncio.Event()
    shutdown_requested = False
    servers = []

    def accept(reader, writer, host, port):
        if stopping.is_set() or (exec_container and len(active) >= MAX_EXEC_STREAMS):
            writer.close()
            return
        task = asyncio.create_task(forward(reader, writer, host, port, exec_container))
        active.add(task)
        task.add_done_callback(active.discard)

    token = secrets.token_hex(32)

    async def stop(reader, writer):
        nonlocal shutdown_requested
        accepted = False
        try:
            received = await asyncio.wait_for(reader.readexactly(len(token)), timeout=1)
            if not stopping.is_set() and hmac.compare_digest(received, token.encode('ascii')):
                accepted = shutdown_requested = True
                stopping.set()
                await stopped.wait()
                writer.write(b'stopped\n')
                await writer.drain()
        except (OSError, EOFError, TimeoutError):
            pass
        finally:
            if accepted:
                reply_sent.set()
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    try:
        for local, host, port in targets:
            servers.append(await asyncio.start_server(
                lambda r, w, h=host, p=port: accept(r, w, h, p), '127.0.0.1', local))
        if not managed:
            await asyncio.gather(*(server.serve_forever() for server in servers))
            return
        if sys.platform != 'win32':
            asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stopping.set)
        control = await asyncio.start_server(stop, '127.0.0.1', 0)
        servers.append(control)
        control_path = Path(path).with_name('relay-control.json')
        pending = control_path.with_suffix('.pending')
        pending.write_text(json.dumps(dict(port=control.sockets[0].getsockname()[1], token=token)))
        pending.replace(control_path)
        await stopping.wait()
    finally:
        for server in servers:
            server.close()
        for task in active:
            task.cancel()
        await asyncio.gather(*active, return_exceptions=True)
        stopped.set()
        if shutdown_requested:
            await reply_sent.wait()
        for server in servers:
            await server.wait_closed()


if __name__ == '__main__':
    asyncio.run(main())
